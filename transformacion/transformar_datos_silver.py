# %%
"""
================================================================================
Nombre del Script: refinar_pacientes_silver.py
Autor: Félix Cárdenas
Fecha de Creación: 2025-05-08
Última Modificación: 2025-05-08
Versión: 1.0.0

Descripción:
Este script forma parte de la capa SILVER del proyecto BigData_Project.
Se encarga de leer los datos crudos de pacientes desde el bucket dev-bronze,
aplicar reglas estrictas de calidad y limpieza con Spark y luego guardar
el resultado como archivo Parquet en el bucket dev-silver.

Dependencias:
- Python >= 3.8
- Librerías: pyspark, pandas, boto3, python-dotenv
"""

# %%
# ================================================================================
# PASO 1: IMPORTACIÓN DE LIBRERÍAS
# ================================================================================
import os
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
import boto3
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_date


# %%
# ================================================================================
# PASO 2: CARGA DE VARIABLES DE ENTORNO
# ================================================================================
load_dotenv("/home/jovyan/.env")

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY   = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_BRONZE      = os.getenv("MINIO_BUCKET_BRONZE")
BUCKET_SILVER      = os.getenv("MINIO_BUCKET_SILVER")


# %%
# ================================================================================
# PASO 3: CREACIÓN DE SPARKSESSION
# ================================================================================
spark = SparkSession.builder \
    .appName("Transformación SILVER") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/home/jovyan/jars/hadoop-aws-3.3.2.jar,/home/jovyan/jars/aws-java-sdk-bundle-1.11.1026.jar") \
    .getOrCreate()


# %%
# ================================================================================
# PASO 4: LECTURA DEL ARCHIVO DEL DÍA DESDE MINIO (bronze) con Boto3 + Spark
# ================================================================================

# Inicializamos cliente S3
s3 = boto3.client("s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Detectar archivo más reciente del día
today = datetime.now().strftime("%Y%m%d")
prefix = "LOCAL_PACIENTES/"
objetos = s3.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix=prefix)

# Buscar el archivo del día
archivo_bronze = None
for obj in objetos.get("Contents", []):
    nombre = obj["Key"]
    if today in nombre and nombre.endswith(".csv"):
        archivo_bronze = nombre
        break

if not archivo_bronze:
    raise FileNotFoundError(f"No se encontró archivo CSV con fecha {today} en {BUCKET_BRONZE}/{prefix}")

# Descargar el archivo temporalmente
ruta_local_tmp = f"/tmp/{Path(archivo_bronze).name}"
with open(ruta_local_tmp, "wb") as f:
    s3.download_fileobj(BUCKET_BRONZE, archivo_bronze, f)

df_spark = spark.read.option("header", True).csv(ruta_local_tmp)


# %%
# ================================================================================
# PASO 5: TRANSFORMACIÓN / CALIDAD DE DATOS CON SPARK
# ================================================================================

df_limpio = df_spark \
    .filter(col("nombre").isNotNull() & (col("nombre") != "")) \
    .filter((col("edad") > 0) & (col("edad") < 120)) \
    .filter(col("obra_social").isNotNull() & (col("obra_social") != "")) \
    .withColumn("fecha_turno", split(col("fecha_turno"), " ").getItem(0)) \
    .withColumn("fecha_turno", to_date("fecha_turno", "yyyy-MM-dd")) \
    .dropna(subset=["fecha_turno"]) \
    .dropDuplicates()



# %%
# ================================================================================  
# PASO 6: CONVERSIÓN A PANDAS Y SUBIDA A MinIO (SILVER)  
# ================================================================================  

# Conversión a pandas 
df_pandas = df_limpio.toPandas()

# Timestamp actual
timestamp = datetime.now().strftime("%Y%m%d%H%M")

# Nombre del archivo refinado
nombre_archivo = f"pacientes_refinados_{timestamp}.parquet"
carpeta_silver = "LOCAL_PACIENTES"
ruta_silver = f"{carpeta_silver}/{nombre_archivo}"

# Guardar en buffer Parquet
buffer = BytesIO()
df_pandas.to_parquet(buffer, index=False)
buffer.seek(0)

# Subir a MinIO
s3.upload_fileobj(buffer, BUCKET_SILVER, ruta_silver)



