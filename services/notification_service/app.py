"""
NotificationService
- Simple demo push service
- Receives Notification messages via gRPC
- Logs them (simulating sending push/SMS/email)
- Also publishes them to RabbitMQ queue "notifications" (optional)
- Supports StreamNotifications for streaming bidirectional testing
"""

import os
import json
import time
import logging
from concurrent import futures

import grpc
import pika
from google.protobuf import empty_pb2
from services.common_lib.protos_generated import (
    notification_pb2,
    notification_pb2_grpc,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification_service")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50056"))

# -------------------------------------------------------------
# RabbitMQ Helper
# -------------------------------------------------------------
def publish_to_queue(queue, payload):
    try:
        conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        ch = conn.channel()
        ch.queue_declare(queue=queue, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        conn.close()
    except Exception as e:
        logger.warning("Rabbit publish failed: %s", e)


# -------------------------------------------------------------
# Notification Service Implementation
# -------------------------------------------------------------
class NotificationService(notification_pb2_grpc.NotificationServiceServicer):

    def Send(self, request, context):
        """
        Receive a Notification message and simulate sending it.
        """
        notif = {
            "to_id": request.to_id,
            "channel": request.channel,
            "title": request.title,
            "body": request.body,
            "meta": request.meta,
            "ts": request.ts,
        }

        logger.info("NOTIFICATION → %s", notif)

        # Publish to RabbitMQ (optional)
        publish_to_queue("notifications", notif)

        return notification_pb2.NotifyAck(ok=True, error="")

    def StreamNotifications(self, request_iterator, context):
        """
        Bidirectional streaming (demo only):
        The client sends Notification messages, we respond with Ack for each.
        """
        for req in request_iterator:
            notif = {
                "to_id": req.to_id,
                "channel": req.channel,
                "title": req.title,
                "body": req.body,
                "meta": req.meta,
                "ts": req.ts,
            }
            logger.info("STREAMED NOTIF → %s", notif)
            publish_to_queue("notifications", notif)
            yield notification_pb2.NotifyAck(ok=True)

    def Health(self, request, context):
        return notification_pb2.NotifyAck(ok=True, error="healthy")


# -------------------------------------------------------------
# gRPC Server
# -------------------------------------------------------------
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notification_pb2_grpc.add_NotificationServiceServicer_to_server(NotificationService(), server)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()

    logger.info(f"NotificationService running on port {GRPC_PORT}")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down NotificationService...")
        server.stop(0)


if __name__ == "__main__":
    serve()
