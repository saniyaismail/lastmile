### FILE: services/rider_service/app.py
"""
RiderService
- Register riders
- Handle pickup requests (publish to RabbitMQ → rider.requests)
- Track ride (simple stub for demo)

Rider DB table:
    riders(rider_id, user_id, name, phone)

RiderService publishes pickup requests:
    {rider_id, station_id, arrival_time, destination}

This integrates with MatchingService.
"""

import os
import time
import json
import logging
from concurrent import futures

import grpc
import pika
from sqlalchemy import create_engine, text

from services.common_lib.protos_generated import (
    rider_pb2,
    rider_pb2_grpc,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rider_service")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lastmile:lastmile@localhost:5432/lastmile")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50057"))

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# -----------------------------------------------------
# DB INIT
# -----------------------------------------------------
def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS riders(
                rider_id TEXT PRIMARY KEY,
                user_id  TEXT,
                name     TEXT,
                phone    TEXT
            );
        """))
    logger.info("RiderService DB initialized.")

# -----------------------------------------------------
# Rabbit Publish
# -----------------------------------------------------

def publish_pickup_request(payload):
    conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    ch = conn.channel()
    ch.queue_declare(queue="rider.requests", durable=True)
    ch.basic_publish(
        exchange="",
        routing_key="rider.requests",
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    conn.close()

# -----------------------------------------------------
# RiderService Implementation
# -----------------------------------------------------
class RiderService(rider_pb2_grpc.RiderServiceServicer):

    def RegisterRider(self, request, context):
        profile = request.profile
        rid = profile.rider_id or f"rider-{int(time.time()*1000)}"

        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO riders(rider_id, user_id, name, phone)
                    VALUES(:rid, :uid, :name, :phone)
                """), {
                    "rid": rid,
                    "uid": profile.user_id,
                    "name": profile.name,
                    "phone": profile.phone,
                })
            logger.info("Registered rider: %s", rid)
            return rider_pb2.RegisterRiderResponse(rider_id=rid, ok=True)

        except Exception as e:
            logger.exception("RegisterRider error: %s", e)
            return rider_pb2.RegisterRiderResponse(ok=False)

    def RequestPickup(self, request, context):
        payload = {
            "rider_id": request.rider_id,
            "station_id": request.station_id,
            "arrival_time": request.arrival_time,
            "destination": request.destination,
            "request_id": request.request_id or f"req-{int(time.time()*1000)}",
        }

        publish_pickup_request(payload)
        logger.info("Published rider pickup request: %s", payload)

        return rider_pb2.RiderResponse(request_id=payload["request_id"], ok=True)

    def TrackRide(self, request, context):
        # Simplified stub (can be linked to TripService later)
        logger.info("TrackRide called for rider %s", request.rider_id)
        return rider_pb2.RiderResponse(request_id=request.request_id, ok=True)

    def Health(self, request, context):
        return rider_pb2.RiderResponse(ok=True)

# -----------------------------------------------------
# gRPC Server
# -----------------------------------------------------

def serve():
    init_db()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    rider_pb2_grpc.add_RiderServiceServicer_to_server(RiderService(), server)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()

    logger.info(f"RiderService running on port {GRPC_PORT}")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down RiderService...")
        server.stop(0)


if __name__ == '__main__':
    serve()
