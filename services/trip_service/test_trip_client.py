"""
A simple client to test TripService manually.
1. Create Trip
2. Get Trip
3. Update Trip
"""
import grpc
import time
from services.common_lib.protos_generated import trip_pb2, trip_pb2_grpc


def main():
    channel = grpc.insecure_channel("localhost:50055")
    stub = trip_pb2_grpc.TripServiceStub(channel)

    print("=== Creating Trip ===")
    create_resp = stub.CreateTrip(trip_pb2.CreateTripRequest(
        trip=trip_pb2.Trip(
            driver_id="drv-test",
            rider_ids=["r1", "r2"],
            origin_station="ST102",
            destination="Downtown",
            status="scheduled",
            start_time=int(time.time()*1000),
            seats_reserved=2,
        )
    ))
    print("CreateTrip response:", create_resp)

    trip_id = create_resp.trip_id

    print("=== Fetching Trip ===")
    get_resp = stub.GetTrip(trip_pb2.GetTripRequest(trip_id=trip_id))
    print("GetTrip response:", get_resp)

    print("=== Updating Trip Status to 'active' ===")
    updated_trip = get_resp.trip
    updated_trip.status = "active"
    update_resp = stub.UpdateTrip(trip_pb2.UpdateTripRequest(trip=updated_trip))
    print("UpdateTrip response:", update_resp)

    print("=== Fetching Trip Again ===")
    get_resp2 = stub.GetTrip(trip_pb2.GetTripRequest(trip_id=trip_id))
    print("GetTrip updated response:", get_resp2)


if __name__ == "__main__":
    main()