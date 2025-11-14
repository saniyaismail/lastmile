"""
Init script to create PostGIS extension and stations table.
Run once after Postgres starts.
"""
import os
from sqlalchemy import create_engine, text

# change the name to postgres in app.py init.py n seed.py instead of localhost to have communication between the container, that is an alias that is used.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lastmile:lastmile@localhost:5432/lastmile")

def main():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # create postgis extension if not exists
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.commit()
        print("PostGIS extension ensured.")

if __name__ == '__main__':
    main()
