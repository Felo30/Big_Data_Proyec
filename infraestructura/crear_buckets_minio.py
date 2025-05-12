# %%
"""
================================================================================
Nombre del Script: crear_buckets_minio.py
Autor: Félix Cárdenas
Fecha de Creación: 2024-05-08
Última Modificación: 2024-05-08
Versión: 1.1.0

Descripción:
Este script crea los buckets necesarios en MinIO para soportar la arquitectura
por capas de un Data Lake:
- dev-bronze: datos crudos
- dev-silver: datos refinados
- dev-diamond: datos validados o enriquecidos
- dev-gold: datos listos para consumo analítico

Dependencias:
- Python >= 3.8
- Librerías: boto3, python-dotenv
"""

# %%
# ================================================================================
# PASO 1: SECCIÓN DE IMPORTACIÓN DE LIBRERÍAS
# ================================================================================
import os
import sys
import logging
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

# %%
# ================================================================================
# PASO 2: CONFIGURACIÓN DE VARIABLES DE ENTORNO
# ================================================================================
load_dotenv("/home/jovyan/.env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

BUCKETS = [
    os.getenv("MINIO_BUCKET_BRONZE"),
    os.getenv("MINIO_BUCKET_SILVER"),
    os.getenv("MINIO_BUCKET_DIAMOND"),
    os.getenv("MINIO_BUCKET_GOLD")
]

# Configuración del logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# %%
# ================================================================================
# PASO 3: DECLARACIÓN DE FUNCIONES
# ================================================================================

# Función Crea un solo bucket en MinIO si no existe previamente.
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
            logger.error(f"Error al crear/verificar bucket '{bucket_name}': {e}")
            sys.exit(1)

# Función Recorre una lista de buckets y los crea si no existen.
def crear_todos_los_buckets(buckets: list) -> None:
    """
    Crea todos los buckets listados si no existen.
    
    Parámetros:
    - buckets (list): lista de nombres de buckets
    """
    logger.info("Iniciando creación de buckets...")
    for bucket in buckets:
        if bucket:
            crear_bucket(bucket)
        else:
            logger.warning("Nombre de bucket no definido en .env")

# %%
# ================================================================================
# PASO 4: EJECUCIÓN DEL SCRIPT
# ================================================================================
if __name__ == '__main__':
    logger.info("Comenzando ejecución del script crear_buckets_minio.py")
    crear_todos_los_buckets(BUCKETS)
    logger.info("Todos los buckets fueron verificados o creados correctamente.")


