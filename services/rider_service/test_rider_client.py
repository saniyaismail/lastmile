"""
Test RiderService
1. Register a rider
2. Send pickup request
"""
import grpc
import time
from services.common_lib.protos_generated import rider_pb2, rider_pb2_grpc

def main():
    channel = grpc.insecure_channel("localhost:50057")
    stub = rider_pb2_grpc.RiderServiceStub(channel)

    print("=== Registering Rider ===")
    reg = stub.RegisterRider(rider_pb2.RegisterRiderRequest(profile=rider_pb2.RiderProfile(
        rider_id="",
        user_id="user-101",
        name="Alice",
        phone="99999",
    )))
    print(reg)

    rider_id = reg.rider_id

    print("=== Sending Pickup Request ===")
    req = stub.RequestPickup(rider_pb2.RiderRequest(
        rider_id=rider_id,
        station_id="ST102",
        arrival_time=int(time.time()*1000) + 60000,
        destination="Downtown",
    ))
    print(req)

if __name__ == "__main__":
    main()
