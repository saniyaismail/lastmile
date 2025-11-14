"""
MatchingService
- Consumes `driver.near_station` and `rider.requests` queues from RabbitMQ
- Keeps in-memory waiting riders per station
- When proximity event arrives, attempts to match riders whose destination == driver's destination
- Creates a trip via TripService gRPC (if available)
- Publishes a `match.found` event to RabbitMQ with trip info
- Calls NotificationService.Send() (if available) to notify rider & driver

Note: For demo simplicity we assume default seats=2. In production you'd query driver state or include available_seats in proximity event.
"""
import os
import json
import time
import logging
import threading
from concurrent import futures

import grpc
import pika
from google.protobuf import empty_pb2

from services.common_lib.protos_generated import (
    matching_pb2,
    matching_pb2_grpc,
    trip_pb2,
    trip_pb2_grpc,
    notification_pb2,
    notification_pb2_grpc,
)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50054"))
TRIP_SERVICE_HOST = os.getenv("TRIP_SERVICE_HOST", "localhost:50054")
NOTIFICATION_SERVICE_HOST = os.getenv("NOTIFICATION_SERVICE_HOST", "localhost:50055")
DEFAULT_SEATS = int(os.getenv("DEFAULT_SEATS", "2"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("matching_service")

# in-memory store of waiting riders: station_id -> list of riders
# rider entry: { 'rider_id', 'arrival_time', 'destination', 'request_id', 'ts' }
station_waiting_riders = {}

# rabbitmq helpers

def get_rabbit_connection():
    params = pika.URLParameters(RABBITMQ_URL)
    return pika.BlockingConnection(params)


def publish_match_event(evt):
    conn = get_rabbit_connection()
    ch = conn.channel()
    ch.queue_declare(queue="match.found", durable=True)
    ch.basic_publish(exchange="", routing_key="match.found", body=json.dumps(evt), properties=pika.BasicProperties(delivery_mode=2))
    conn.close()


# TripService client helper
def create_trip_via_tripservice(driver_id, rider_ids, station_id, destination):
    try:
        channel = grpc.insecure_channel(TRIP_SERVICE_HOST)
        stub = trip_pb2_grpc.TripServiceStub(channel)
        trip = trip_pb2.Trip(driver_id=driver_id, rider_ids=rider_ids, origin_station=station_id, destination=destination, status="scheduled", seats_reserved=len(rider_ids), start_time=int(time.time()*1000))
        req = trip_pb2.CreateTripRequest(trip=trip)
        resp = stub.CreateTrip(req, timeout=5)
        if resp and resp.ok:
            return resp.trip_id
    except Exception as e:
        logger.warning("TripService call failed: %s", e)
    return None


# Notification client helper
def notify_entity(entity_id, title, body, meta=None):
    try:
        channel = grpc.insecure_channel(NOTIFICATION_SERVICE_HOST)
        stub = notification_pb2_grpc.NotificationServiceStub(channel)
        n = notification_pb2.Notification(to_id=entity_id, channel="push", title=title, body=body, meta=json.dumps(meta or {}), ts=int(time.time()*1000))
        ack = stub.Send(n, timeout=3)
        return ack.ok
    except Exception as e:
        logger.warning("Notify fail: %s", e)
        return False


# RabbitMQ callbacks

def on_rider_request(ch, method, properties, body):
    try:
        payload = json.loads(body)
        station_id = payload.get("station_id")
        rider = {
            "rider_id": payload.get("rider_id"),
            "arrival_time": payload.get("arrival_time"),
            "destination": payload.get("destination"),
            "request_id": payload.get("request_id") or f"req-{int(time.time()*1000)}",
            "ts": int(time.time()*1000),
        }
        station_waiting_riders.setdefault(station_id, []).append(rider)
        logger.info("Rider queued at %s: %s", station_id, rider)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.exception("Error processing rider.request: %s", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def on_proximity_event(ch, method, properties, body):
    try:
        payload = json.loads(body)
        driver_id = payload.get("driver_id")
        station_id = payload.get("station_id")
        ts = payload.get("ts")
        logger.info("Proximity: driver %s near station %s", driver_id, station_id)

        # look for waiting riders at station
        waiting = station_waiting_riders.get(station_id, [])
        if not waiting:
            logger.info("No waiting riders at %s", station_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # choose riders whose destination matches (simple FIFO)
        matched = []
        for r in list(waiting):
            if len(matched) >= DEFAULT_SEATS:
                break
            # In this simple implementation we don't have driver destination info
            # so match any rider (or implement destination filtering if available)
            matched.append(r)

        if not matched:
            logger.info("No matched riders for driver %s at %s", driver_id, station_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        rider_ids = [r["rider_id"] for r in matched]
        destination = matched[0].get("destination")

        # create trip via TripService (best-effort)
        trip_id = create_trip_via_tripservice(driver_id, rider_ids, station_id, destination)

        # publish match event
        evt = {
            "driver_id": driver_id,
            "rider_ids": rider_ids,
            "station_id": station_id,
            "destination": destination,
            "trip_id": trip_id or f"trip-{int(time.time()*1000)}",
            "ts": ts,
        }
        publish_match_event(evt)
        logger.info("Published match event: %s", evt)

        # notify participants
        for rid in rider_ids:
            notify_entity(rid, "Ride matched", f"Driver {driver_id} will pick you at {station_id}", meta={"trip_id": evt["trip_id"]})
        notify_entity(driver_id, "Riders matched", f"Riders {','.join(rider_ids)} waiting at {station_id}", meta={"trip_id": evt["trip_id"]})

        # remove matched riders from waiting list
        station_waiting_riders[station_id] = [r for r in waiting if r["rider_id"] not in rider_ids]

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.exception("Error processing proximity event: %s", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_rabbit_consumers():
    conn = get_rabbit_connection()
    ch = conn.channel()
    ch.queue_declare(queue="rider.requests", durable=True)
    ch.queue_declare(queue="driver.near_station", durable=True)
    ch.queue_declare(queue="match.found", durable=True)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue="rider.requests", on_message_callback=on_rider_request)
    ch.basic_consume(queue="driver.near_station", on_message_callback=on_proximity_event)

    logger.info("MatchingService consuming rider.requests and driver.near_station")
    ch.start_consuming()


# gRPC server (optional) - provide health endpoints
class MatchingServiceServicer(matching_pb2_grpc.MatchingServiceServicer):
    def FindMatches(self, request, context):
        # Allow explicit RPC-based matching (not used in event-driven flow)
        station_id = request.station_id
        driver_id = request.driver_id
        riders = request.rider_ids
        # simple response
        return matching_pb2.MatchResponse(accepted=True, trip_id=f"trip-{int(time.time()*1000)}")

    def StreamMatches(self, request_iterator, context):
        for req in request_iterator:
            yield matching_pb2.MatchResponse(accepted=True, trip_id=f"trip-{int(time.time()*1000)}")

    def Health(self, request, context):
        return matching_pb2.MatchResponse(accepted=True, trip_id="healthy")


def serve():
    # start rabbit consumer in background thread
    t = threading.Thread(target=start_rabbit_consumers, daemon=True)
    t.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    matching_pb2_grpc.add_MatchingServiceServicer_to_server(MatchingServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logger.info(f"MatchingService gRPC server started on {GRPC_PORT}")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down MatchingService")
        server.stop(0)


if __name__ == '__main__':
    serve()