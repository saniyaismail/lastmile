#!/bin/bash

set -e  # exit on error

NAMESPACE="lastmile"

echo "======================================="
echo "LASTMILE CLOUD DEPLOYMENT STARTED"
echo "======================================="

# ---------------------------------------------------------
# 1. Create namespace
# ---------------------------------------------------------
echo "Creating namespace: $NAMESPACE"
kubectl apply -f infra/k8s/namespace.yaml

# ---------------------------------------------------------
# 2. Apply secrets + configmap
# ---------------------------------------------------------
echo "Applying secrets + configmap..."
kubectl apply -f infra/k8s/secret-configmap.yaml

# ---------------------------------------------------------
# 3. Deploy Postgres (StatefulSet)
# ---------------------------------------------------------
echo "Deploying Postgres (StatefulSet)..."
kubectl apply -f infra/k8s/postgres-statefulset.yaml

echo "Waiting for Postgres to be ready..."
kubectl -n $NAMESPACE rollout status statefulset/postgres

# ---------------------------------------------------------
# 4. Deploy RabbitMQ
# ---------------------------------------------------------
echo "Deploying RabbitMQ..."
kubectl apply -f infra/k8s/rabbitmq-deployment.yaml

echo "Waiting for RabbitMQ to be ready..."
kubectl -n $NAMESPACE rollout status deploy/rabbitmq

# ---------------------------------------------------------
# 5. Deploy ALL microservices (in dependency order)
# ---------------------------------------------------------

echo "Deploying Station Service..."
kubectl apply -f services/station_service/k8s/station-deployment.yaml

echo "Deploying Driver Service..."
kubectl apply -f services/driver_service/k8s/driver-deployment.yaml

echo "Deploying Location Service..."
kubectl apply -f services/location_service/k8s/location-deployment.yaml

echo "Deploying Matching Service..."
kubectl apply -f services/matching_service/k8s/matching-deployment.yaml

echo "Deploying Trip Service..."
kubectl apply -f services/trip_service/k8s/trip-deployment.yaml

echo "Deploying Notification Service..."
kubectl apply -f services/notification_service/k8s/notification-deployment.yaml

echo "Deploying Rider Service..."
kubectl apply -f services/rider_service/k8s/rider-deployment.yaml

echo "Deploying User Service..."
kubectl apply -f services/user_service/k8s/user-deployment.yaml

# ---------------------------------------------------------
# 6. Deploy Horizontal Pod Autoscaler for Matching Service
# ---------------------------------------------------------
echo "Applying HorizontalPodAutoscaler for MatchingService..."
kubectl apply -f infra/k8s/matching-hpa.yaml

# ---------------------------------------------------------
# 7. Summary
# ---------------------------------------------------------

echo ""
echo "======================================="
echo "LASTMILE DEPLOYMENT COMPLETED"
echo "======================================="

echo "Check pods:"
echo "kubectl -n lastmile get pods"

echo "Postgres:"
echo "kubectl -n lastmile port-forward svc/postgres 5432:5432"

echo "RabbitMQ UI:"
echo "kubectl -n lastmile port-forward svc/rabbitmq 15672:15672"
echo "Open: http://localhost:15672 (guest/guest)"

echo "To inspect logs:"
echo "kubectl -n lastmile logs -l app=matching-service -f"

echo "======================================="
