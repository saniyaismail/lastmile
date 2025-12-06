"""
UserService
- Register users (driver or rider)
- Login (returns JWT or placeholder token)
- GetUser profiles

DB table:
    users(user_id, name, phone, email, role, password_hash)

For demo:
- Passwords stored as plain text or simple hash (not secure). Replace with bcrypt in production.
"""

import os
import time
import json
import logging
import hashlib
from concurrent import futures

import grpc
from sqlalchemy import create_engine, text
from google.protobuf import empty_pb2

from services.common_lib.protos_generated import (
    user_pb2,
    user_pb2_grpc,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("user_service")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lastmile:lastmile@localhost:5432/lastmile")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50058"))

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# -----------------------------------------------------
# DB INIT
# -----------------------------------------------------
def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users(
                user_id TEXT PRIMARY KEY,
                name    TEXT,
                phone   TEXT,
                email   TEXT,
                role    TEXT,
                password_hash TEXT
            );
        """))
    logger.info("UserService DB initialized.")

# -----------------------------------------------------
# Helper
# -----------------------------------------------------
def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

# -----------------------------------------------------
# UserService Implementation
# -----------------------------------------------------
class UserService(user_pb2_grpc.UserServiceServicer):

    def RegisterUser(self, request, context):
        profile = request.profile
        user_id = profile.user_id or f"user-{int(time.time()*1000)}"
        pw_hash = hash_password(request.password)

        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO users(user_id, name, phone, email, role, password_hash)
                    VALUES(:uid, :name, :phone, :email, :role, :pw)
                """), {
                    "uid": user_id,
                    "name": profile.name,
                    "phone": profile.phone,
                    "email": profile.email,
                    "role": profile.role,
                    "pw": pw_hash,
                })
            token = f"token-{user_id}"  # demo token
            logger.info("Registered user: %s", user_id)
            return user_pb2.RegisterUserResponse(user_id=user_id, ok=True, token=token)

        except Exception as e:
            logger.exception("RegisterUser error: %s", e)
            return user_pb2.RegisterUserResponse(ok=False)

    def Login(self, request, context):
        email = request.email
        pw_hash = hash_password(request.password)

        try:
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT user_id, password_hash FROM users WHERE email=:email"
                ), {"email": email}).fetchone()

                if not row:
                    return user_pb2.LoginResponse(token="", user_id="")

                if row.password_hash != pw_hash:
                    return user_pb2.LoginResponse(token="", user_id="")

                token = f"token-{row.user_id}"
                return user_pb2.LoginResponse(token=token, user_id=row.user_id)

        except Exception as e:
            logger.exception("Login error: %s", e)
            return user_pb2.LoginResponse(token="", user_id="")

    def GetUser(self, request, context):
        uid = request.user_id
        try:
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT * FROM users WHERE user_id=:uid"
                ), {"uid": uid}).fetchone()

                if not row:
                    return user_pb2.GetUserResponse()

                profile = user_pb2.UserProfile(
                    user_id=row.user_id,
                    name=row.name,
                    phone=row.phone,
                    email=row.email,
                    role=row.role,
                )
                return user_pb2.GetUserResponse(profile=profile)
        except Exception as e:
            logger.exception("GetUser error: %s", e)
            return user_pb2.GetUserResponse()

    def Health(self, request, context):
        return user_pb2.GetUserResponse(profile=user_pb2.UserProfile(user_id="healthy"))

# -----------------------------------------------------
# gRPC Server
# -----------------------------------------------------
def serve():
    init_db()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()

    logger.info(f"UserService running on port {GRPC_PORT}")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down UserService...")
        server.stop(0)


if __name__ == "__main__":
    serve()
