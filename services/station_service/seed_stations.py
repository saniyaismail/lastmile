"""
Simple script to seed stations into DB.
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from services.station_service.models import Station as StationModel

# NOTE: we'll include a small models file here to reuse in seed; if not available, we inline a minimal insert.

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lastmile:lastmile@localhost:5432/lastmile")

# Simple inline insert if models not importable
from sqlalchemy import text

engine = create_engine(DATABASE_URL)
with engine.connect() as conn:
    stations = [
        ("ST101", "MG Road", 12.975, 77.605),
        ("ST102", "Cubbon Park", 12.9759, 77.601),
        ("ST103", "Trinity", 12.9718, 77.6380),
    ]
    for sid, name, lat, lng in stations:
        # upsert
        conn.execute(text(
            "INSERT INTO stations (station_id, name, lat, lng, geom) VALUES (:sid, :name, :lat, :lng, ST_SetSRID(ST_MakePoint(:lng,:lat),4326))"
        ), {"sid": sid, "name": name, "lat": lat, "lng": lng})
    conn.commit()
print("Seeded stations")
