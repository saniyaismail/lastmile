"""
TripService
- Provides CreateTrip, UpdateTrip, GetTrip gRPC methods
- Stores trips in Postgres (with PostGIS already available)
- Publishes trip lifecycle events to RabbitMQ (trip.created, trip.updated)
- Used by MatchingService when a match is created
"""
import os
import json
import time
import logging
from concurrent import futures

import grpc
import pika
from sqlalchemy import create_engine, text
from services.common_lib.protos_generated import trip_pb2, trip_pb2_grpc
from google.protobuf import empty_pb2

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lastmile:lastmile@localhost:5432/lastmile")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50055"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("trip_service")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Rabbit helper

def publish_event(queue_name, payload):
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    ch.queue_declare(queue=queue_name, durable=True)
    ch.basic_publish(exchange="", routing_key=queue_name, body=json.dumps(payload), properties=pika.BasicProperties(delivery_mode=2))
    conn.close()


# DB INIT (if not created yet)
def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS trips (
            trip_id        TEXT PRIMARY KEY,
            driver_id      TEXT,
            rider_ids      TEXT,   -- JSON list
            origin_station TEXT,
            destination    TEXT,
            status         TEXT,
            start_time     BIGINT,
            end_time       BIGINT,
            seats_reserved INT
        );
        """))
    logger.info("TripService DB initialized.")


class TripServiceServicer(trip_pb2_grpc.TripServiceServicer):
    def CreateTrip(self, request, context):
        trip = request.trip
        trip_id = trip.trip_id or f"trip-{int(time.time()*1000)}"
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO trips(trip_id, driver_id, rider_ids, origin_station, destination, status, start_time, end_time, seats_reserved)
                    VALUES(:trip_id, :driver_id, :rider_ids, :origin_station, :destination, :status, :start_time, :end_time, :seats_reserved)
                """), {
                    "trip_id": trip_id,
                    "driver_id": trip.driver_id,
                    "rider_ids": json.dumps(list(trip.rider_ids)),
                    "origin_station": trip.origin_station,
                    "destination": trip.destination,
                    "status": trip.status,
                    "start_time": trip.start_time,
                    "end_time": trip.end_time,
                    "seats_reserved": trip.seats_reserved,
                })
            evt = {
                "event": "trip.created",
                "trip_id": trip_id,
                "driver_id": trip.driver_id,
                "rider_ids": list(trip.rider_ids),
                "status": trip.status,
                "station": trip.origin_station,
                "destination": trip.destination,
            }
            publish_event("trip.created", evt)
            logger.info("Created trip: %s", evt)
            return trip_pb2.CreateTripResponse(trip_id=trip_id, ok=True)
        except Exception as e:
            logger.exception("CreateTrip DB error: %s", e)
            return trip_pb2.CreateTripResponse(ok=False, reason=str(e))

    def UpdateTrip(self, request, context):
        trip = request.trip
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE trips SET
                        driver_id=:driver_id,
                        rider_ids=:rider_ids,
                        origin_station=:origin_station,
                        destination=:destination,
                        status=:status,
                        start_time=:start_time,
                        end_time=:end_time,
                        seats_reserved=:seats_reserved
                    WHERE trip_id=:trip_id
                """), {
                    "trip_id": trip.trip_id,
                    "driver_id": trip.driver_id,
                    "rider_ids": json.dumps(list(trip.rider_ids)),
                    "origin_station": trip.origin_station,
                    "destination": trip.destination,
                    "status": trip.status,
                    "start_time": trip.start_time,
                    "end_time": trip.end_time,
                    "seats_reserved": trip.seats_reserved,
                })
            evt = {
                "event": "trip.updated",
                "trip_id": trip.trip_id,
                "status": trip.status,
            }
            publish_event("trip.updated", evt)
            logger.info("Updated trip: %s", evt)
            return trip_pb2.UpdateTripResponse(ok=True)
        except Exception as e:
            logger.exception("UpdateTrip DB error: %s", e)
            return trip_pb2.UpdateTripResponse(ok=False)

    def GetTrip(self, request, context):
        try:
            with engine.connect() as conn:
                row = conn.execute(text("SELECT * FROM trips WHERE trip_id=:tid"), {"tid": request.trip_id}).fetchone()
                if not row:
                    return trip_pb2.GetTripResponse()
                trip = trip_pb2.Trip(
                    trip_id=row.trip_id,
                    driver_id=row.driver_id,
                    rider_ids=json.loads(row.rider_ids),
                    origin_station=row.origin_station,
                    destination=row.destination,
                    status=row.status,
                    start_time=row.start_time,
                    end_time=row.end_time,
                    seats_reserved=row.seats_reserved,
                )
                return trip_pb2.GetTripResponse(trip=trip)
        except Exception as e:
            logger.exception("GetTrip DB error: %s", e)
            return trip_pb2.GetTripResponse()

    def Health(self, request, context):
        return trip_pb2.CreateTripResponse(ok=True, trip_id="healthy")


def serve():
    init_db()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    trip_pb2_grpc.add_TripServiceServicer_to_server(TripServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logger.info(f"TripService gRPC server started on {GRPC_PORT}")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()