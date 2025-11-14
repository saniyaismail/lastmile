"""
Consume match.found events to verify matches
"""
import pika
import json

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
params = pika.URLParameters(RABBITMQ_URL)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue="match.found", durable=True)
print("Waiting for match.found events")
for method, props, body in ch.consume('match.found'):
    print("Match event:", body)
    ch.basic_ack(method.delivery_tag)