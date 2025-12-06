#!/bin/bash

# Colors
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}Starting All LastMile Microservices...${NC}"

# Kill old processes
pkill -f user_service/app.py
pkill -f rider_service/app.py
pkill -f driver_service/app.py
pkill -f station_service/app.py
pkill -f location_service/app.py
pkill -f matching_service/app.py
pkill -f notification_service/app.py
pkill -f trip_service/app.py

sleep 1

# Start each service in background (logs go to logs/*.log)
mkdir -p logs

echo "Starting UserService..."
python services/user_service/app.py > logs/user.log 2>&1 &
sleep 1

echo "Starting RiderService..."
python services/rider_service/app.py > logs/rider.log 2>&1 &
sleep 1

echo "Starting DriverService..."
python services/driver_service/app.py > logs/driver.log 2>&1 &
sleep 1

echo "Starting StationService..."
python services/station_service/app.py > logs/station.log 2>&1 &
sleep 1

echo "Starting LocationService..."
python services/location_service/app.py > logs/location.log 2>&1 &
sleep 1

echo "Starting TripService..."
python services/trip_service/app.py > logs/trip.log 2>&1 &
sleep 1

echo "Starting NotificationService..."
python services/notification_service/app.py > logs/notification.log 2>&1 &
sleep 1

echo "Starting MatchingService..."
python services/matching_service/app.py > logs/matching.log 2>&1 &
sleep 1

echo -e "${GREEN}All services started successfully!${NC}"
echo "Logs are available in ./logs/"
