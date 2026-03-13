#!/bin/bash
set -e

echo "Starting ShopStream stack..."
docker compose up -d zookeeper kafka minio postgres

echo "Waiting for services to be healthy..."
sleep 30

echo "Setting up MinIO buckets..."
docker compose up minio-setup

echo "Seeding PostgreSQL..."
python scripts/seed_postgres.py

echo "Starting Airflow..."
docker compose up -d airflow-init
sleep 20
docker compose up -d airflow-webserver airflow-scheduler

echo "Starting Spark..."
docker compose up -d spark spark-worker

echo ""
echo "ShopStream is running!"
echo "  Kafka UI:         http://localhost:8080"
echo "  MinIO Console:    http://localhost:9001  (minioadmin / minioadmin)"
echo "  Airflow:          http://localhost:8082  (admin / admin)"
echo "  Spark UI:         http://localhost:8081"
echo "  PostgreSQL:       localhost:5432"
echo ""
echo "To generate data: python -m data_generator.kafka_producer --mode continuous"
