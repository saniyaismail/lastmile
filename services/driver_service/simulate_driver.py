"""
Simple driver simulator that connects to DriverService.StreamLocation and streams
LocationUpdate protobuf messages at fixed intervals.

Usage example:
  python simulate_driver.py --driver-id drv-1 --host localhost --port 50052
"""
import argparse
import time
import grpc

from services.common_lib.protos_generated import driver_pb2, driver_pb2_grpc


def generate_route_points(start_lat, start_lng, steps=10, step=0.0005):
    pts = []
    lat = start_lat
    lng = start_lng
    for i in range(steps):
        pts.append((lat + i*step, lng + i*step))
    return pts


def run(driver_id, host, port, station_id, destination, interval):
    target = f"{host}:{port}"
    channel = grpc.insecure_channel(target)
    stub = driver_pb2_grpc.DriverServiceStub(channel)

    # register driver (one-off)
    profile = driver_pb2.DriverProfile(driver_id=driver_id, user_id=f"user-{driver_id}", name="Sim Driver", phone="0000000000", vehicle_no="KA01-TEST")
    stub.RegisterDriver(driver_pb2.RegisterDriverRequest(profile=profile))

    # update route
    route = driver_pb2.Route(route_id=f"route-{driver_id}", station_ids=[station_id], waypoints=[f"{0},{0}"])
    stub.UpdateRoute(driver_pb2.DriverRouteRequest(driver_id=driver_id, route=route, destination=destination, available_seats=2))

    # open stream
    locations = generate_route_points(12.9710, 77.5940, steps=12, step=0.0005)

    def location_generator():
        ts_base = int(time.time()*1000)
        for i,(lat,lng) in enumerate(locations):
            loc = driver_pb2.LocationUpdate(
                driver_id=driver_id,
                lat=lat,
                lng=lng,
                timestamp=int(time.time()*1000),
                status="enroute",
                station_id=station_id if i == len(locations)-3 else "",
                available_seats=2,
                destination=destination,
                eta_ms=2*60*1000 if i == len(locations)-3 else 5*60*1000,
            )
            print("sending:", loc)
            yield loc
            time.sleep(interval)

    ack = stub.StreamLocation(location_generator())
    print("stream ended, ack:", ack)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--driver-id', default='drv-sim-1')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default=50052, type=int)
    parser.add_argument('--station-id', default='ST101')
    parser.add_argument('--destination', default='Downtown')
    parser.add_argument('--interval', default=2, type=int)
    args = parser.parse_args()
    run(args.driver_id, args.host, args.port, args.station_id, args.destination, args.interval)