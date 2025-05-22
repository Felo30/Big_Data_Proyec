"""
================================================================================
Nombre del Script: crear_buckets_minio.py
Autor: Félix Cárdenas
Fecha de Creación: 2024-05-08
Última Modificación: 2025-05-16
Versión: 1.2.0

Descripción:
Este script crea los buckets necesarios en MinIO para soportar la arquitectura
por capas de un Data Lake:
- dev-bronze: datos crudos
- dev-silver: datos refinados
- dev-diamond: datos validados o enriquecidos
- dev-gold: datos listos para consumo analítico

Uso:
- Directamente en notebook o script (llamando a run_creacion_buckets())
- Importado desde un DAG de Airflow como PythonOperator

Dependencias:
- Python >= 3.8
- Librerías: boto3, python-dotenv
================================================================================
"""

# ================================================================================
# PASO 1: IMPORTACIÓN DE LIBRERÍAS
# ================================================================================
import os
import sys
import logging
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

# ================================================================================
# PASO 2: CARGA VARIABLES DE ENTORNO (.env)
# ================================================================================
load_dotenv("/opt/airflow/.env")  # compatible con DAGs en contenedor

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

BUCKETS = [
    os.getenv("MINIO_BUCKET_BRONZE"),
    os.getenv("MINIO_BUCKET_SILVER"),
    os.getenv("MINIO_BUCKET_DIAMOND"),
    os.getenv("MINIO_BUCKET_GOLD")
]

# ================================================================================
# PASO 3: CONFIGURACIÓN DE LOGGING
# ================================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ================================================================================
# PASO 4: FUNCIONES
# ================================================================================

def crear_bucket(bucket_name: str) -> None:
    """
    Crea un bucket en MinIO si no existe.

    Parámetros:
    - bucket_name (str): nombre del bucket a crear.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"El bucket '{bucket_name}' ya existe.")
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            s3.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' creado.")
        else:
            logger.error(f"Error al verificar/crear bucket '{bucket_name}': {e}")
            sys.exit(1)

def crear_todos_los_buckets(buckets: list) -> None:
    """
    Crea todos los buckets definidos en la lista.

    Parámetros:
    - buckets (list): lista de nombres de buckets
    """
    logger.info("Iniciando proceso de creación de buckets...")
    for bucket in buckets:
        if bucket:
            crear_bucket(bucket)
        else:
            logger.warning("Bucket no definido en archivo .env.")
    logger.info("Proceso de creación de buckets finalizado.")

def run_creacion_buckets():
    """
    Función principal para ejecutar desde Airflow (PythonOperator).
    """
    logger.info("Ejecutando run_creacion_buckets desde Airflow...")
    crear_todos_los_buckets(BUCKETS)

# ================================================================================
# PASO 5: EJECUCIÓN DIRECTA (Notebook o Script)
# ================================================================================
if __name__ == '__main__':
    run_creacion_buckets()
