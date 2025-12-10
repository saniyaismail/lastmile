#!/bin/bash
set -e

NS="lastmile"

echo "==============================================="
echo "      DEPLOYING LASTMILE TO KUBERNETES"
echo "==============================================="

# -----------------------------------------------------------
# 1) Create namespace
# -----------------------------------------------------------
echo "[1] Creating namespace '$NS'..."
kubectl get ns $NS >/dev/null 2>&1 || kubectl create namespace $NS

# -----------------------------------------------------------
# 2) Apply infrastructure (RabbitMQ + Postgres)
# -----------------------------------------------------------
echo "[2] Deploying RabbitMQ + Postgres..."
kubectl apply -n $NS -f infra/k8s/postgres.yaml
kubectl apply -n $NS -f infra/k8s/rabbitmq.yaml

# Give DB & MQ time to start
echo "Waiting 20s for DB + RabbitMQ to initialize..."
sleep 20

# -----------------------------------------------------------
# 3) Deploy all microservices
# -----------------------------------------------------------
echo "[3] Deploying microservices..."

kubectl apply -n $NS -f services/station_service/k8s/station-deployment.yaml
kubectl apply -n $NS -f services/driver_service/k8s/driver-deployment.yaml
kubectl apply -n $NS -f services/rider_service/k8s/rider-deployment.yaml
kubectl apply -n $NS -f services/location_service/k8s/location-deployment.yaml
kubectl apply -n $NS -f services/matching_service/k8s/matching-deployment.yaml
kubectl apply -n $NS -f services/trip_service/k8s/trip-deployment.yaml
kubectl apply -n $NS -f services/notification_service/k8s/notification-deployment.yaml
kubectl apply -n $NS -f services/user_service/k8s/user-deployment.yaml

# -----------------------------------------------------------
# 4) Wait for pods to be ready
# -----------------------------------------------------------
echo "[4] Waiting for all pods to be ready..."
kubectl wait --namespace $NS --for=condition=ready pod --all --timeout=180s

# -----------------------------------------------------------
# 5) Seed Station DB
# -----------------------------------------------------------
echo "[5] Seeding StationService database..."
kubectl -n $NS run station-seeder --rm -it \
  --env="DATABASE_URL=postgresql://lastmile:lastmile@postgres:5432/lastmile" \
  --image=saniyaismail999/lastmile-station:1.0 \
  --restart=Never -- \
  bash -c "python services/station_service/init_db.py && python services/station_service/seed_stations.py"

# -----------------------------------------------------------
# 6) Show pod status
# -----------------------------------------------------------
echo "[6] Deployment Complete! Current pods:"
kubectl -n $NS get pods

echo "==============================================="
echo "         LASTMILE DEPLOYMENT FINISHED!"
echo "==============================================="
