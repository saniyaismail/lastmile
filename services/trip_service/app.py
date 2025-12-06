"""
TripService — FINAL VERSION
Handles:
- CreateTrip (scheduled)
- UpdateTrip (active / completed / canceled)
- GetTrip
Publishes:
    trip.created
    trip.updated
"""

import os
import json
import time
import logging
from concurrent import futures

import grpc
import pika
from sqlalchemy import create_engine, text
from google.protobuf import empty_pb2

from services.common_lib.protos_generated import (
    trip_pb2,
    trip_pb2_grpc,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("trip_service")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lastmile:lastmile@localhost:5432/lastmile")
RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
GRPC_PORT = int(os.getenv("GRPC_PORT", 50055))

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# -------------------------------------------------------
# RabbitMQ Publisher
# -------------------------------------------------------

def publish_event(queue_name, event_dict):
    """Safely publish JSON events."""
    try:
        conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
        ch = conn.channel()
        ch.queue_declare(queue=queue_name, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps(event_dict),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        conn.close()
    except Exception as e:
        logger.error(f"Failed to publish {queue_name}: {e}")


# -------------------------------------------------------
# DB Init
# -------------------------------------------------------

def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trips (
                trip_id TEXT PRIMARY KEY,
                driver_id TEXT,
                rider_ids TEXT,
                origin_station TEXT,
                destination TEXT,
                status TEXT,
                start_time BIGINT,
                end_time BIGINT,
                seats_reserved INT
            );
        """))
    logger.info("TripService DB initialized.")


# -------------------------------------------------------
# Helper: fetch trip row from DB
# -------------------------------------------------------

def fetch_trip(trip_id):
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT trip_id, driver_id, rider_ids, origin_station, destination,
                   status, start_time, end_time, seats_reserved
            FROM trips WHERE trip_id=:trip_id
        """), {"trip_id": trip_id}).fetchone()
    return row


# -------------------------------------------------------
# TripService gRPC Implementation
# -------------------------------------------------------

class TripService(trip_pb2_grpc.TripServiceServicer):

    # ---------------------------------------------------
    # CREATE TRIP
    # ---------------------------------------------------
    def CreateTrip(self, request, context):
        trip = request.trip
        trip_id = f"trip-{int(time.time()*1000)}"

        try:
            rider_ids_str = ",".join(list(trip.rider_ids))

            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO trips(
                        trip_id, driver_id, rider_ids,
                        origin_station, destination,
                        status, start_time, seats_reserved
                    )
                    VALUES(
                        :trip_id, :driver_id, :rider_ids,
                        :origin_station, :destination,
                        'scheduled', :start_time, :seats_reserved
                    )
                """), {
                    "trip_id": trip_id,
                    "driver_id": trip.driver_id,
                    "rider_ids": rider_ids_str,
                    "origin_station": trip.origin_station,
                    "destination": trip.destination,
                    "start_time": int(time.time()*1000),
                    "seats_reserved": trip.seats_reserved,
                })

            # Publish event
            event = {
                "event": "trip.created",
                "trip_id": trip_id,
                "driver_id": trip.driver_id,
                "rider_ids": list(trip.rider_ids),
                "destination": trip.destination,
                "status": "scheduled",
                "ts": int(time.time()*1000)
            }
            publish_event("trip.created", event)
            logger.info(f"Published trip.created: {event}")

            return trip_pb2.CreateTripResponse(trip_id=trip_id, ok=True)

        except Exception as e:
            logger.exception("CreateTrip error")
            return trip_pb2.CreateTripResponse(ok=False, reason=str(e))


    # ---------------------------------------------------
    # UPDATE TRIP
    # ---------------------------------------------------
    def UpdateTrip(self, request, context):
        incoming = request.trip
        trip_id = incoming.trip_id
        new_status = incoming.status
        ts = int(time.time()*1000)

        try:
            # Update DB
            with engine.begin() as conn:
                if new_status == "completed":
                    conn.execute(text("""
                        UPDATE trips
                        SET status=:status, end_time=:end_time
                        WHERE trip_id=:trip_id
                    """), {"status": new_status, "end_time": ts, "trip_id": trip_id})
                else:
                    conn.execute(text("""
                        UPDATE trips
                        SET status=:status
                        WHERE trip_id=:trip_id
                    """), {"status": new_status, "trip_id": trip_id})

            # Fetch full trip info from DB for event publishing
            row = fetch_trip(trip_id)
            if not row:
                return trip_pb2.UpdateTripResponse(ok=False)

            rider_ids = row.rider_ids.split(",") if row.rider_ids else []

            event = {
                "event": "trip.updated",
                "trip_id": row.trip_id,
                "driver_id": row.driver_id,
                "rider_ids": rider_ids,
                "destination": row.destination,
                "status": row.status,
                "ts": ts
            }

            publish_event("trip.updated", event)
            logger.info(f"Published trip.updated: {event}")

            return trip_pb2.UpdateTripResponse(ok=True)

        except Exception as e:
            logger.exception("UpdateTrip error")
            return trip_pb2.UpdateTripResponse(ok=False)


    # ---------------------------------------------------
    # GET TRIP
    # ---------------------------------------------------
    def GetTrip(self, request, context):
        row = fetch_trip(request.trip_id)
        if not row:
            return trip_pb2.GetTripResponse()

        rider_ids = row.rider_ids.split(",") if row.rider_ids else []

        trip = trip_pb2.Trip(
            trip_id=row.trip_id,
            driver_id=row.driver_id,
            rider_ids=rider_ids,
            origin_station=row.origin_station,
            destination=row.destination,
            status=row.status,
            start_time=row.start_time,
            end_time=row.end_time,
            seats_reserved=row.seats_reserved,
        )

        return trip_pb2.GetTripResponse(trip=trip)


    def Health(self, request, context):
        return trip_pb2.CreateTripResponse(ok=True)


# -------------------------------------------------------
# Start gRPC Server
# -------------------------------------------------------

def serve():
    init_db()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    trip_pb2_grpc.add_TripServiceServicer_to_server(TripService(), server)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logger.info(f"TripService started on port {GRPC_PORT}")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
