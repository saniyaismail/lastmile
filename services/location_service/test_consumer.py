"""
Simple script to consume driver.near_station queue and print events
for local verification.
"""
import pika
import os
import json

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

params = pika.URLParameters(RABBITMQ_URL)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue="driver.near_station", durable=True)

print("Waiting for proximity events on driver.near_station")
for method, properties, body in ch.consume("driver.near_station"):
    print("Event:", body)
    ch.basic_ack(method.delivery_tag)
