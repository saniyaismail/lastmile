"""
StationService gRPC server (Python).
Implements: GetStation, ListStations, Health
Uses SQLAlchemy + GeoAlchemy2 to store station locations (PostGIS)
"""
import os
import logging
import time
from concurrent import futures

from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from geoalchemy2 import Geometry

import grpc

# import generated protos
from services.common_lib.protos_generated import station_pb2, station_pb2_grpc
from google.protobuf import empty_pb2

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lastmile:lastmile@localhost:5432/lastmile")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("station_service")

Base = declarative_base()

class Station(Base):
    __tablename__ = "stations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String(64), unique=True, nullable=False)
    name = Column(String(256), nullable=False)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326))


def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


SessionLocal = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)


class StationServiceServicer(station_pb2_grpc.StationServiceServicer):

    def __init__(self):
        self.engine = get_engine()
        Base.metadata.create_all(self.engine)

    def GetStation(self, request, context):
        station_id = request.station_id
        session = SessionLocal()
        try:
            row = session.query(Station).filter(Station.station_id == station_id).first()
            if not row:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("station not found")
                return station_pb2.StationResponse()
            station = station_pb2.Station(
                station_id=row.station_id,
                name=row.name,
                lat=row.lat,
                lng=row.lng,
                nearby_places=[],
            )
            return station_pb2.StationResponse(station=station)
        finally:
            session.close()

    def ListStations(self, request, context):
        session = SessionLocal()
        try:
            if request.station_ids:
                rows = session.query(Station).filter(Station.station_id.in_(request.station_ids)).all()
            else:
                rows = session.query(Station).all()
            stations = []
            for r in rows:
                stations.append(station_pb2.Station(
                    station_id=r.station_id,
                    name=r.name,
                    lat=r.lat,
                    lng=r.lng,
                    nearby_places=[],
                ))
            return station_pb2.StationListResponse(stations=stations)
        finally:
            session.close()

    def Health(self, request, context):
        # simple health: return an empty StationResponse
        # implement a lightweight DB check
        try:
            engine = self.engine
            conn = engine.connect()
            conn.execute("SELECT 1")
            conn.close()
            # return first station as a sample response if exists
            session = SessionLocal()
            row = session.query(Station).first()
            session.close()
            if row:
                st = station_pb2.Station(station_id=row.station_id, name=row.name, lat=row.lat, lng=row.lng)
                return station_pb2.StationResponse(station=st)
            else:
                return station_pb2.StationResponse()
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return station_pb2.StationResponse()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    station_pb2_grpc.add_StationServiceServicer_to_server(StationServiceServicer(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logger.info(f"StationService gRPC server started on port {GRPC_PORT}")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        server.stop(0)


if __name__ == "__main__":
    serve()

