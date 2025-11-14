"""
DriverService gRPC server (Python).
Implements RegisterDriver, UpdateRoute, StreamLocation.
StreamLocation consumes a stream of LocationUpdate messages from the driver client
and publishes each message to RabbitMQ queue `driver.locations` in JSON.
"""
import os
import json
import time
import logging
from concurrent import futures

import grpc
import pika

from services.common_lib.protos_generated import driver_pb2, driver_pb2_grpc
from google.protobuf import empty_pb2

#change local host to rabbitmq when communicating between containers
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50052"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("driver_service")

# simple in-memory store for drivers; production should use DB
DRIVERS = {}
ROUTES = {}


def get_rabbit_channel():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue="driver.locations", durable=True)
    return connection, channel


class DriverServiceServicer(driver_pb2_grpc.DriverServiceServicer):

    def RegisterDriver(self, request, context):
        profile = request.profile
        driver_id = profile.driver_id or f"drv-{int(time.time()*1000)}"
        DRIVERS[driver_id] = {
            "user_id": profile.user_id,
            "name": profile.name,
            "phone": profile.phone,
            "vehicle_no": profile.vehicle_no,
        }
        logger.info("Registered driver %s", driver_id)
        return driver_pb2.RegisterDriverResponse(driver_id=driver_id, ok=True)

    def UpdateRoute(self, request, context):
        driver_id = request.driver_id
        route = request.route
        ROUTES[driver_id] = {
            "route_id": route.route_id,
            "station_ids": list(route.station_ids),
            "waypoints": list(route.waypoints),
            "destination": request.destination,
            "available_seats": request.available_seats,
        }
        logger.info("Updated route for %s -> %s", driver_id, request.destination)
        return driver_pb2.DriverRouteResponse(ok=True)

    def StreamLocation(self, request_iterator, context):
        # synchronous server streaming handler: iterate over messages and publish
        conn, ch = get_rabbit_channel()
        try:
            for loc in request_iterator:
                payload = {
                    "driver_id": loc.driver_id,
                    "lat": loc.lat,
                    "lng": loc.lng,
                    "timestamp": loc.timestamp,
                    "status": loc.status,
                    "station_id": loc.station_id,
                    "available_seats": loc.available_seats,
                    "destination": loc.destination,
                    "eta_ms": loc.eta_ms,
                }
                body = json.dumps(payload)
                ch.basic_publish(exchange="", routing_key="driver.locations", body=body, properties=pika.BasicProperties(delivery_mode=2))
                logger.debug("Published location for %s -> %s", loc.driver_id, body)
        except grpc.RpcError as e:
            logger.error("Stream closed: %s", e)
        finally:
            conn.close()
        return driver_pb2.Ack(ok=True)

    def Health(self, request, context):
        return driver_pb2.Ack(ok=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    driver_pb2_grpc.add_DriverServiceServicer_to_server(DriverServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logger.info(f"DriverService gRPC server started on port {GRPC_PORT}")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        server.stop(0)


if __name__ == "__main__":
    serve()
