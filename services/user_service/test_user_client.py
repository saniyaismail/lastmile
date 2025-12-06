"""
Test UserService
1. Register user
2. Login
3. Fetch profile
"""
import grpc
from services.common_lib.protos_generated import user_pb2, user_pb2_grpc

def main():
    channel = grpc.insecure_channel("localhost:50058")
    stub = user_pb2_grpc.UserServiceStub(channel)

    print("=== Register User ===")
    reg = stub.RegisterUser(user_pb2.RegisterUserRequest(
        profile=user_pb2.UserProfile(
            user_id="",
            name="Saniya",
            phone="99999",
            email="saniya@example.com",
            role="rider"
        ),
        password="pass123"
    ))
    print(reg)

    print("=== Login ===")
    login = stub.Login(user_pb2.LoginRequest(
        email="saniya@example.com",
        password="pass123"
    ))
    print(login)

    print("=== GetUser ===")
    getu = stub.GetUser(user_pb2.GetUserRequest(user_id=login.user_id))
    print(getu)

if __name__ == "__main__":
    main()