import time
import json
import pika
import grpc
from services.common_lib.protos_generated import (
    driver_pb2, driver_pb2_grpc,
    rider_pb2, rider_pb2_grpc,
    trip_pb2, trip_pb2_grpc,
)

RABBIT = "amqp://guest:guest@localhost:5672/"

def rabbit_channel():
    conn = pika.BlockingConnection(pika.URLParameters(RABBIT))
    return conn, conn.channel()

# -------------------------------------------------------------------
# 1) Publish rider request
# -------------------------------------------------------------------
def publish_rider_request(rider_id, station_id, destination):
    conn, ch = rabbit_channel()
    ch.queue_declare(queue="rider.requests", durable=True)

    req = {
        "rider_id": rider_id,
        "arrival_time": int(time.time() * 1000),
        "station_id": station_id,
        "destination": destination,
    }

    ch.basic_publish(
        exchange="",
        routing_key="rider.requests",
        body=json.dumps(req).encode(),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    conn.close()
    print(f"[TEST] Published rider request: {req}")


# -------------------------------------------------------------------
# 2) Publish driver proximity event
# -------------------------------------------------------------------
def publish_driver_proximity(driver_id, station_id, seats, destination):
    conn, ch = rabbit_channel()
    ch.queue_declare(queue="driver.near_station", durable=True)

    event = {
        "driver_id": driver_id,
        "station_id": station_id,
        "available_seats": seats,
        "destination": destination,
        "ts": int(time.time()*1000)
    }

    ch.basic_publish(
        exchange="",
        routing_key="driver.near_station",
        body=json.dumps(event).encode(),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    conn.close()
    print(f"[TEST] Published driver proximity: {event}")


# -------------------------------------------------------------------
# 3) Consume match.found event
# -------------------------------------------------------------------
def consume_match_found(timeout=5):
    print("[TEST] Waiting for match.found...")

    conn, ch = rabbit_channel()
    ch.queue_declare(queue="match.found", durable=True)

    method_frame, header, body = ch.basic_get("match.found", auto_ack=True)

    deadline = time.time() + timeout
    while not method_frame and time.time() < deadline:
        method_frame, header, body = ch.basic_get("match.found", auto_ack=True)
        time.sleep(0.2)

    if not method_frame:
        print("[❌ TEST FAILED] No match event received.")
        return None

    event = json.loads(body.decode())
    print("[TEST] Match Found:", event)
    return event


# -------------------------------------------------------------------
# 4) Mark trip as completed
# -------------------------------------------------------------------
def complete_trip(trip_id):
    print(f"[TEST] Completing trip {trip_id}...")
    channel = grpc.insecure_channel("localhost:50055")
    stub = trip_pb2_grpc.TripServiceStub(channel)

    update = trip_pb2.Trip(trip_id=trip_id, status="completed", rider_ids=[])
    req = trip_pb2.UpdateTripRequest(trip=update)
    res = stub.UpdateTrip(req)
    print("[TEST] Trip completed:", res.ok)
    return res.ok


# -------------------------------------------------------------------
# 5) Full E2E Test Flow
# -------------------------------------------------------------------
def run_e2e_test():
    print("\n================= E2E TEST START =================\n")

    DRIVER = "drv-1"
    DEST = "Downtown"

    # -----------------------------------------------------------
    # Rider1 requests pickup at ST102
    # -----------------------------------------------------------
    publish_rider_request("rider-1", "ST102", DEST)
    time.sleep(1)

    # -----------------------------------------------------------
    # Driver gets near ST102 with 2 seats
    # -----------------------------------------------------------
    publish_driver_proximity(DRIVER, "ST102", seats=2, destination=DEST)

    event = consume_match_found()
    if not event:
        return

    trip_id = event["trip_id"]

    # -----------------------------------------------------------
    # Rider2 requests at next station ST103
    # -----------------------------------------------------------
    publish_rider_request("rider-2", "ST103", DEST)
    time.sleep(1)

    # Driver reaches ST103 with only 1 seat left
    publish_driver_proximity(DRIVER, "ST103", seats=1, destination=DEST)

    event2 = consume_match_found()
    if not event2:
        return

    # -----------------------------------------------------------
    # Now driver should have 0 seats
    # Rider requesting again should NOT match
    # -----------------------------------------------------------
    publish_rider_request("rider-3", "ST104", DEST)
    time.sleep(1)

    publish_driver_proximity(DRIVER, "ST104", seats=0, destination=DEST)

    no_match = consume_match_found(timeout=2)
    if no_match:
        print("[❌ TEST FAILED] A match happened when seats were 0!")
    else:
        print("[TEST] Correct: No match with 0 seats")

    # -----------------------------------------------------------
    # COMPLETE TRIP
    # -----------------------------------------------------------
    complete_trip(trip_id)
    time.sleep(2)

    # -----------------------------------------------------------
    # Seats should now RESET to DEFAULT_SEATS
    # New rider should be matched
    # -----------------------------------------------------------
    publish_rider_request("rider-4", "ST102", DEST)
    time.sleep(1)

    publish_driver_proximity(DRIVER, "ST102", seats=2, destination=DEST)

    event3 = consume_match_found()
    if event3:
        print("[TEST] Correct: New trip matched after seat reset!")
    else:
        print("[❌ TEST FAILED] No match after seat reset.")

    print("\n================= E2E TEST COMPLETE =================\n")


if __name__ == "__main__":
    run_e2e_test()
