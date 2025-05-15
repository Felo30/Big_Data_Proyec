# %%
"""
================================================================================
Nombre del Script: guardar_csv_bronze.py
Autor: Félix Cárdenas
Fecha de Creación: 2025-05-08
Última Modificación: 2025-05-15
Versión: 2.0.0

Descripción:
Este script forma parte de la capa BRONZE del proyecto BigData_Project.
Lee un archivo CSV crudo con Spark y lo guarda directamente en el bucket
`dev-bronze` de MinIO usando `s3a://`, sin pasar por pandas ni boto3.

El archivo se guarda en una única partición para facilitar futuros procesos
de renombre o carga incremental.

Estructura de destino en MinIO:
  s3a://{BUCKET}/LOCAL_{DOMINIO}/{nombre_archivo_base}_{timestamp}/part-00000...

Dependencias:
- Python >= 3.8
- pyspark, python-dotenv
"""

# %%
# ================================================================================
# PASO 1: IMPORTACIÓN DE LIBRERÍAS
# ================================================================================
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# %%
# ================================================================================
# PASO 2: CARGA DE VARIABLES DE ENTORNO
# ================================================================================

# Cargar las variables definidas en el archivo .env
load_dotenv("/home/jovyan/.env")

ruta_csv_local     = "/home/jovyan/datos/csv/pacientes_crudo.csv"
MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY   = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_BRONZE      = os.getenv("MINIO_BUCKET_BRONZE")

nombre_archivo_base = Path(ruta_csv_local).stem
dominio             = nombre_archivo_base.split("_")[0].lower()
carpeta_destino     = f"LOCAL_{dominio.upper()}"
timestamp           = datetime.now().strftime("%Y%m%d%H%M")
carpeta_output      = f"{nombre_archivo_base}_{timestamp}"
output_path         = f"s3a://{BUCKET_BRONZE}/{carpeta_destino}/{carpeta_output}"

# %%
# ================================================================================
# PASO 3: LECTURA CON SPARK Y CONVERSIÓN A PANDAS
# ================================================================================
spark = SparkSession.builder \
    .appName("Guardar CSV en MinIO con Spark") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.901.jar") \
    .getOrCreate()


# %%
# ================================================================================
# PASO 4: LECTURA Y GUARDADO
# ================================================================================

df = spark.read.option("header", True).csv(ruta_csv_local)
df.repartition(1).write.mode("overwrite").option("header", True).csv(output_path)


