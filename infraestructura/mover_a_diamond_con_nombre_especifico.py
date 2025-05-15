# %%
"""
================================================================================
Nombre del Script: mover_a_diamond_con_nombre_especifico.py
Autor: Félix Cárdenas
Fecha de Creación: 2025-05-15
Versión: 1.0.0

Descripción:
Este script busca el archivo Parquet más reciente generado por Spark
en la capa SILVER (dev-silver/LOCAL_PACIENTES/...), y lo copia a la capa
DIAMOND con un nombre legible y limpio: pacientes_refinados_{timestamp}.parquet.

No elimina nada del origen.
================================================================================
"""

# %%
# ================================================================================
# PASO 1: IMPORTACIÓN DE LIBRERÍAS Y VARIABLES DE ENTORNO
# ================================================================================
import os
from datetime import datetime
from dotenv import load_dotenv
import boto3

load_dotenv("/home/jovyan/.env")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_SILVER     = os.getenv("MINIO_BUCKET_SILVER")
BUCKET_DIAMOND    = os.getenv("MINIO_BUCKET_DIAMOND")

# %%
# ================================================================================
# PASO 2: CONEXIÓN A MINIO Y DETECCIÓN DE ARCHIVO MÁS RECIENTE
# ================================================================================
s3 = boto3.client("s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

prefix = "LOCAL_PACIENTES/pacientes_refinados_"
objetos = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefix)

# Buscar el part-*.parquet más reciente
archivos = [
    obj["Key"] for obj in objetos.get("Contents", [])
    if "part-" in obj["Key"] and obj["Key"].endswith(".parquet")
]

if not archivos:
    raise FileNotFoundError("No se encontró ningún archivo .parquet en SILVER.")

archivo_mas_reciente = sorted(archivos, reverse=True)[0]

# Extraer timestamp del path
timestamp = archivo_mas_reciente.split("_")[-1].replace(".parquet/part-", "").split("-")[0]
nuevo_nombre = f"LOCAL_PACIENTES/pacientes_refinados_{timestamp}.parquet"

# %%
# ================================================================================
# PASO 3: COPIAR ARCHIVO A DIAMOND CON NOMBRE LIMPIO
# ================================================================================
s3.copy_object(
    Bucket=BUCKET_DIAMOND,
    CopySource={'Bucket': BUCKET_SILVER, 'Key': archivo_mas_reciente},
    Key=nuevo_nombre
)


