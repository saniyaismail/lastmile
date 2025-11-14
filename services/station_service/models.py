"""
SQLAlchemy models used by station_service and helpers
"""
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.orm import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class Station(Base):
    __tablename__ = 'stations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String(64), unique=True, nullable=False)
    name = Column(String(256), nullable=False)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    geom = Column(Geometry(geometry_type='POINT', srid=4326))