# %%
"""
================================================================================
Nombre del Script: guardar_csv_bronze.py
Autor: Félix Cárdenas
Fecha de Creación: 2025-05-08
Última Modificación: 2025-05-08
Versión: 1.0.0

Descripción:
Este script forma parte de la capa BRONZE del proyecto BigData_Project.
Lee un archivo CSV local con Spark, lo convierte a Pandas y lo guarda en el
bucket `dev-bronze` de MinIO con una estructura basada en origen y nombre del archivo.

Dependencias:
- Python >= 3.8
- Librerías: pandas, boto3, dotenv, pyspark
"""

# %%
# ================================================================================
# PASO 1: IMPORTACIÓN DE LIBRERÍAS
# ================================================================================
import os
import logging
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pathlib import Path

# %%
# ================================================================================
# PASO 2: CARGA DE VARIABLES DE ENTORNO
# ================================================================================

# Cargar las variables definidas en el archivo .env
load_dotenv("/home/jovyan/.env")

# Ruta local del archivo a cargar
ruta_csv_local     = "/home/jovyan/datos/csv/pacientes_crudo.csv"

# Parámetros MinIO
MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY   = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_BRONZE      = os.getenv("MINIO_BUCKET_BRONZE")

# ===> Nombre base del archivo (sin extensión)
nombre_archivo_base = Path(ruta_csv_local).stem 

# ===> Dominio extraído del nombre: lo que esté antes del guion bajo
dominio = nombre_archivo_base.split("_")[0].lower()  

# ===> Carpeta MinIO: siempre LOCAL_{dominio.upper()}
carpeta_destino = f"LOCAL_{dominio.upper()}" 

# ===> Timestamp actual
timestamp = datetime.now().strftime("%Y%m%d%H%M")

# ===> Nombre final del archivo
nombre_archivo = f"{nombre_archivo_base}_{timestamp}.csv"  

# ===> Ruta final en MinIO (clave)
key_minio = f"{carpeta_destino}/{nombre_archivo}"

# %%
# ================================================================================
# PASO 3: LECTURA CON SPARK Y CONVERSIÓN A PANDAS
# ================================================================================
spark = SparkSession.builder \
    .appName("Guardar CSV en MinIO") \
    .getOrCreate()

df_spark = spark.read.option("header", True).csv(ruta_csv_local)
df_pandas = df_spark.toPandas()


# %%
# ================================================================================
# PASO 4: GUARDADO EN MINIO CON BOTO3
# ================================================================================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

buffer = BytesIO()
df_pandas.to_csv(buffer, index=False)
buffer.seek(0)

s3.upload_fileobj(buffer, BUCKET_BRONZE, key_minio)


