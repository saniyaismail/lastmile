"""
MatchingService — FINAL LOGIC
-------------------------------------------------------
Driver:
  - has one route
  - can pick multiple riders from multiple stations
  - destination is fixed
  - drops all riders only at final destination

Riders:
  - must match driver destination
  - matched only when driver is near their station

Seats:
  - decremented on match
  - reset ONLY when TripService reports trip.updated with status="completed"

Events Consumed:
  - rider.requests
  - driver.near_station
  - trip.updated

Events Published:
  - match.found
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("matching_service")

RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50054"))
TRIP_SERVICE_HOST = os.getenv("TRIP_SERVICE_HOST", "localhost:50055")
NOTIFICATION_SERVICE_HOST = os.getenv("NOTIFICATION_SERVICE_HOST", "localhost:50056")
DEFAULT_SEATS = int(os.getenv("DEFAULT_SEATS", 2))

# memory stores
station_waiting_riders = {}   # station_id -> list of riders
driver_seat_state = {}        # driver_id -> available seats
driver_destination_state = {} # driver_id -> destination string


# ---------------------------------------------------------
# RabbitMQ Helpers
# ---------------------------------------------------------
def rabbit_conn():
    return pika.BlockingConnection(pika.URLParameters(RABBIT_URL))


def publish_match_event(ev):
    conn = rabbit_conn()
    ch = conn.channel()
    ch.queue_declare(queue="match.found", durable=True)

    ch.basic_publish(
        exchange="",
        routing_key="match.found",
        body=json.dumps(ev),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    conn.close()


# ---------------------------------------------------------
# GRPC Clients
# ---------------------------------------------------------

def trip_create(driver_id, rider_ids, station_id, destination):
    try:
        channel = grpc.insecure_channel(TRIP_SERVICE_HOST)
        stub = trip_pb2_grpc.TripServiceStub(channel)

        trip = trip_pb2.Trip(
            driver_id=driver_id,
            rider_ids=rider_ids,
            origin_station=station_id,
            destination=destination,
            status="scheduled",
            seats_reserved=len(rider_ids),
            start_time=int(time.time()*1000),
        )

        req = trip_pb2.CreateTripRequest(trip=trip)
        resp = stub.CreateTrip(req, timeout=5)

        if resp.ok:
            return resp.trip_id
    except Exception as e:
        logger.warning("TripService CreateTrip failed: %s", e)

    # fallback trip_id
    return f"trip-{int(time.time()*1000)}"


def notify(to_id, title, body, meta=None):
    try:
        channel = grpc.insecure_channel(NOTIFICATION_SERVICE_HOST)
        stub = notification_pb2_grpc.NotificationServiceStub(channel)

        msg = notification_pb2.Notification(
            to_id=to_id,
            channel="push",
            title=title,
            body=body,
            meta=json.dumps(meta or {}),
            ts=int(time.time()*1000)
        )
        stub.Send(msg, timeout=3)
    except Exception as e:
        logger.warning("NotificationService failed: %s", e)


# ---------------------------------------------------------
# Rabbit Consumers
# ---------------------------------------------------------

def on_rider_request(ch, method, props, body):
    try:
        data = json.loads(body)
        station = data["station_id"]

        rider = {
            "rider_id": data["rider_id"],
            "arrival_time": data["arrival_time"],
            "destination": data["destination"],
            "request_id": data.get("request_id", f"req-{int(time.time()*1000)}"),
        }

        station_waiting_riders.setdefault(station, []).append(rider)
        logger.info(f"[RIDER] Rider waiting at {station}: {rider}")

        ch.basic_ack(method.delivery_tag)
    except Exception as e:
        logger.exception("Error rider.request")
        ch.basic_nack(method.delivery_tag, requeue=False)


def on_driver_near_station(ch, method, props, body):
    try:
        data = json.loads(body)
        driver_id = data["driver_id"]
        station_id = data["station_id"]
        driver_dest = data.get("destination")
        seats_from_event = int(data.get("available_seats", DEFAULT_SEATS))

        # store the driver's intended destination
        if driver_dest:
            driver_destination_state[driver_id] = driver_dest

        # Initialize seat state if new driver
        if driver_id not in driver_seat_state:
            driver_seat_state[driver_id] = seats_from_event
            logger.info(f"[DRIVER] Init seats {driver_id} = {seats_from_event}")
        else:
            # sync seats if event has lower seat count (safety)
            if seats_from_event < driver_seat_state[driver_id]:
                driver_seat_state[driver_id] = seats_from_event

        seats = driver_seat_state[driver_id]

        # no seats available
        if seats <= 0:
            logger.info(f"[MATCH] Driver {driver_id} has no seats left.")
            ch.basic_ack(method.delivery_tag)
            return

        waiting = station_waiting_riders.get(station_id, [])
        if not waiting:
            logger.info(f"[MATCH] No riders waiting at {station_id}.")
            ch.basic_ack(method.delivery_tag)
            return

        # match riders whose destination matches driver's destination
        matched = []
        for r in list(waiting):
            if len(matched) >= seats:
                break
            if driver_dest and r["destination"] == driver_dest:
                matched.append(r)

        if not matched:
            logger.info(f"[MATCH] No riders matched driver {driver_id} at {station_id}")
            ch.basic_ack(method.delivery_tag)
            return

        rider_ids = [r["rider_id"] for r in matched]
        final_destination = driver_dest

        # create trip
        trip_id = trip_create(driver_id, rider_ids, station_id, final_destination)

        # publish match event
        event = {
            "driver_id": driver_id,
            "rider_ids": rider_ids,
            "station_id": station_id,
            "trip_id": trip_id,
            "destination": final_destination,
            "ts": int(time.time()*1000),
        }

        publish_match_event(event)
        logger.info(f"[MATCH] Match created: {event}")

        # notify riders + driver
        for rid in rider_ids:
            notify(rid, "Ride Matched", f"Driver {driver_id} will pick you at {station_id}", {"trip_id": trip_id})
        notify(driver_id, "Riders Matched", f"Riders: {','.join(rider_ids)}")

        # remove matched riders
        station_waiting_riders[station_id] = [r for r in waiting if r["rider_id"] not in rider_ids]

        # decrement seat count
        old = driver_seat_state[driver_id]
        driver_seat_state[driver_id] = max(0, old - len(rider_ids))
        logger.info(f"[SEATS] Driver {driver_id}: {old} → {driver_seat_state[driver_id]}")

        ch.basic_ack(method.delivery_tag)

    except Exception as e:
        logger.exception("Error driver.near_station")
        ch.basic_nack(method.delivery_tag, requeue=False)


def on_trip_updated(ch, method, props, body):
    try:
        data = json.loads(body)
        if data.get("event") == "trip.updated" and data.get("status") == "completed":
            driver_id = data.get("driver_id")
            if driver_id:
                driver_seat_state[driver_id] = DEFAULT_SEATS
                logger.info(f"[RESET] Trip completed → reset seats for {driver_id} to {DEFAULT_SEATS}")

        ch.basic_ack(method.delivery_tag)
    except Exception:
        logger.exception("Error trip.updated")
        ch.basic_nack(method.delivery_tag, requeue=False)


# ---------------------------------------------------------
# Queue Setup
# ---------------------------------------------------------

def start_consumers():
    conn = rabbit_conn()
    ch = conn.channel()

    ch.queue_declare(queue="rider.requests", durable=True)
    ch.queue_declare(queue="driver.near_station", durable=True)
    ch.queue_declare(queue="trip.updated", durable=True)
    ch.queue_declare(queue="match.found", durable=True)

    ch.basic_qos(prefetch_count=1)

    ch.basic_consume("rider.requests", on_rider_request)
    ch.basic_consume("driver.near_station", on_driver_near_station)
    ch.basic_consume("trip.updated", on_trip_updated)

    logger.info("MatchingService consuming rider.requests, driver.near_station, trip.updated")
    ch.start_consuming()


# ---------------------------------------------------------
# GRPC server (Health only)
# ---------------------------------------------------------

class MatchingServiceGRPC(matching_pb2_grpc.MatchingServiceServicer):
    def Health(self, req, ctx):
        return matching_pb2.MatchResponse(accepted=True, trip_id="OK")


def serve():
    t = threading.Thread(target=start_consumers, daemon=True)
    t.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    matching_pb2_grpc.add_MatchingServiceServicer_to_server(MatchingServiceGRPC(), server)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()

    logger.info(f"MatchingService started on {GRPC_PORT}")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
