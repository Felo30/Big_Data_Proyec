#!/bin/bash

echo "Deteniendo contenedores..."

docker compose \
  -f docker-compose.postgres.yml \
  -f docker-compose.minio.yml \
  -f docker-compose.jupyter.yml \
  -f docker-compose.spark.yml \
  -f docker-compose.airflow.yml \
  down

echo "Contenedores detenidos con éxito."
