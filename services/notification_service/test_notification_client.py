### FILE: services/notification_service/test_notification_client.py
"""
Simple test client for NotificationService
"""
import grpc
import time
from services.common_lib.protos_generated import notification_pb2, notification_pb2_grpc

def main():
    channel = grpc.insecure_channel("localhost:50056")
    stub = notification_pb2_grpc.NotificationServiceStub(channel)

    resp = stub.Send(notification_pb2.Notification(
        to_id="user-123",
        channel="push",
        title="Test Notification",
        body="Hello from NotificationService!",
        meta="{}",
        ts=int(time.time()*1000),
    ))

    print("Send Response:", resp)

if __name__ == "__main__":
    main()
