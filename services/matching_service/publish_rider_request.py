"""
Simple script to publish a rider request to RabbitMQ for testing
"""
import pika
import json
import time
import argparse

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
params = pika.URLParameters(RABBITMQ_URL)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue="rider.requests", durable=True)

parser = argparse.ArgumentParser()
parser.add_argument('--rider-id', default='rider-1')
parser.add_argument('--station-id', default='ST102')
parser.add_argument('--destination', default='Downtown')
args = parser.parse_args()

payload = {
    "rider_id": args.rider_id,
    "station_id": args.station_id,
    "arrival_time": int(time.time()*1000) + 2*60*1000,
    "destination": args.destination,
}
ch.basic_publish(exchange="", routing_key="rider.requests", body=json.dumps(payload), properties=pika.BasicProperties(delivery_mode=2))
print("Published rider request:", payload)
conn.close()


