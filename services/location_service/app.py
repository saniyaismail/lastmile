### FILE: services/location_service/app.py
"""
LocationService
- Consumes `driver.locations` RabbitMQ queue
- For each location: compute nearest station using PostGIS (stations table)
- If distance < PROXIMITY_THRESHOLD_M (default 150m) -> publish to `driver.near_station` queue
- Exposes gRPC endpoints: ReportLocation, StreamProximity, Health
"""
import os
import json
import time
import logging
import threading
from concurrent import futures

import grpc
import pika
from sqlalchemy import create_engine, text
from services.common_lib.protos_generated import location_pb2, location_pb2_grpc
from google.protobuf import empty_pb2


#change the url 
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lastmile:lastmile@localhost:5432/lastmile")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50053"))
PROXIMITY_THRESHOLD_M = int(os.getenv("PROXIMITY_THRESHOLD_M", "200"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("location_service")

# simple DB engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# RabbitMQ helper

def get_rabbit_connection():
    params = pika.URLParameters(RABBITMQ_URL)
    return pika.BlockingConnection(params)


def publish_proximity_event(evt):
    conn = get_rabbit_connection()
    ch = conn.channel()
    ch.queue_declare(queue="driver.near_station", durable=True)
    ch.basic_publish(exchange="", routing_key="driver.near_station", body=json.dumps(evt), properties=pika.BasicProperties(delivery_mode=2))
    conn.close()


class LocationServiceServicer(location_pb2_grpc.LocationServiceServicer):
    def ReportLocation(self, request, context):
        # Allow other services to report locations via gRPC (optional)
        # We'll run the same proximity detection logic here synchronously
        lat = request.lat
        lng = request.lng
        driver_id = request.driver_id
        ts = request.ts
        station_row = find_nearest_station(lat, lng)
        if station_row:
            station_id, distance_m = station_row
            if distance_m <= PROXIMITY_THRESHOLD_M:
                evt = {"driver_id": driver_id, "station_id": station_id, "distance_m": distance_m, "ts": ts}
                publish_proximity_event(evt)
                logger.info("Published proximity event via ReportLocation: %s", evt)
        return empty_pb2.Empty()

    def StreamProximity(self, request_iterator, context):
        # This streaming API echoes back proximity events for provided locations
        for loc in request_iterator:
            lat = loc.lat
            lng = loc.lng
            driver_id = loc.driver_id
            ts = loc.ts
            station_row = find_nearest_station(lat, lng)
            if station_row:
                station_id, distance_m = station_row
                if distance_m <= PROXIMITY_THRESHOLD_M:
                    yield location_pb2.ProximityEvent(driver_id=driver_id, station_id=station_id, distance_m=distance_m, ts=ts)

    def Health(self, request, context):
        return empty_pb2.Empty()


# DB helper: find nearest station and distance using PostGIS
def find_nearest_station(lat, lng):
    try:
        with engine.connect() as conn:
            # Use geography for meters accuracy
            sql = text(
                "SELECT station_id, ST_DistanceSphere(geom::geometry, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)) as distance_m "
                "FROM stations ORDER BY distance_m ASC LIMIT 1"
            )
            res = conn.execute(sql, {"lat": lat, "lng": lng}).fetchone()
            if res:
                return res[0], float(res[1])
    except Exception as e:
        logger.exception("DB error in find_nearest_station: %s", e)
    return None


# RabbitMQ consumer: listen on driver.locations and process
def on_driver_location(ch, method, properties, body):
    try:
        payload = json.loads(body)
        driver_id = payload.get("driver_id")
        lat = payload.get("lat")
        lng = payload.get("lng")
        ts = payload.get("timestamp") or int(time.time()*1000)
        station_row = find_nearest_station(lat, lng)
        if station_row:
            station_id, distance_m = station_row
            logger.info("Nearest station for %s is %s (%.1fm)", driver_id, station_id, distance_m)
            if distance_m <= PROXIMITY_THRESHOLD_M:
                evt = {"driver_id": driver_id, "station_id": station_id, "distance_m": distance_m, "ts": ts}
                publish_proximity_event(evt)
                logger.info("Published proximity event: %s", evt)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.exception("Error processing driver.location: %s", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_rabbit_consumer():
    conn = get_rabbit_connection()
    ch = conn.channel()
    ch.queue_declare(queue="driver.locations", durable=True)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue="driver.locations", on_message_callback=on_driver_location)
    logger.info("LocationService consuming driver.locations")
    ch.start_consuming()


def serve():
    # start rabbit consumer on background thread
    t = threading.Thread(target=start_rabbit_consumer, daemon=True)
    t.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    location_pb2_grpc.add_LocationServiceServicer_to_server(LocationServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logger.info(f"LocationService gRPC server started on port {GRPC_PORT}")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down LocationService")
        server.stop(0)


if __name__ == '__main__':
    serve()
