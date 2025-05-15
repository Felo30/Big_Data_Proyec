# %%
"""
 Nombre del Script: refinar_pacientes_silver.py
 Autor: Félix Cárdenas
 Fecha de Creación: 2025-05-08
 Última Modificación: 2025-05-15
 Versión: 2.0.0

 Descripción:
 Este script forma parte de la capa SILVER del proyecto BigData_Project.
 Lee el archivo más reciente desde MinIO (en formato CSV dentro de carpeta con timestamp)
 y aplica reglas de calidad con Spark. Luego guarda el resultado como archivo Parquet
 en el bucket dev-silver, también con nombre versionado.
"""

# %%
# ================================================================================
# PASO 1: IMPORTACIÓN DE LIBRERÍAS
# ================================================================================
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date


# %%
# ================================================================================
# PASO 2: CARGA DE VARIABLES DE ENTORNO
# ================================================================================
load_dotenv("/home/jovyan/.env")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_BRONZE     = os.getenv("MINIO_BUCKET_BRONZE")
BUCKET_SILVER     = os.getenv("MINIO_BUCKET_SILVER")


# %%
# ================================================================================
# PASO 3: CREACIÓN DE SPARKSESSION
# ================================================================================
spark = SparkSession.builder \
    .appName("Transformacion Silver") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.901.jar") \
    .getOrCreate()


# %%
# ================================================================================  
# PASO 4: DETECCIÓN DE ARCHIVO MÁS RECIENTE EN MinIO (BRONZE)
# ================================================================================  

s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY)

prefix = "LOCAL_PACIENTES/"
today = datetime.now().strftime("%Y%m%d")

objetos = s3.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix=prefix)
archivos = [
    obj["Key"] for obj in objetos.get("Contents", [])
    if today in obj["Key"] and "part-" in obj["Key"]
]

if not archivos:
    raise FileNotFoundError(f"No se encontró ningún archivo CSV para hoy: {today}")

archivo_mas_reciente = sorted(archivos, reverse=True)[0]
ruta_s3a = f"s3a://{BUCKET_BRONZE}/{archivo_mas_reciente}"

# %%
# ================================================================================  
# PASO 5: LECTURA DEL ARCHIVO + TRANSFORMACIÓN DE CALIDAD CON SPARK
# ================================================================================  

df = spark.read.option("header", True).csv(ruta_s3a)

df_limpio = df \
    .filter(col("nombre").isNotNull() & (col("nombre") != "")) \
    .filter((col("edad") > 0) & (col("edad") < 120)) \
    .filter(col("obra_social").isNotNull() & (col("obra_social") != "")) \
    .withColumn("fecha_turno", split(col("fecha_turno"), " ").getItem(0)) \
    .withColumn("fecha_turno", to_date("fecha_turno", "yyyy-MM-dd")) \
    .dropna(subset=["fecha_turno"]) \
    .dropDuplicates()


# %%
# ================================================================================  
# PASO 6: GUARDADO FINAL COMO PARQUET EN MinIO (SILVER)
# ================================================================================  

nombre_archivo = f"pacientes_refinados_{datetime.now().strftime('%Y%m%d%H%M')}.parquet"
ruta_output = f"s3a://{BUCKET_SILVER}/LOCAL_PACIENTES/{nombre_archivo}"

df_limpio.repartition(1).write.mode("overwrite").parquet(ruta_output)



