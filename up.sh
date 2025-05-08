#!/bin/bash

echo "Levantando contenedores..."

docker compose \
  -f docker-compose.postgres.yml \
  -f docker-compose.minio.yml \
  -f docker-compose.jupyter.yml \
  -f docker-compose.spark.yml \
  up -d

echo "Contenedores levantados con éxito."
