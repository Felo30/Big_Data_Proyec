# %%
"""
================================================================================
Nombre del Script: cargar_pacientes_diamond.py
Autor: Félix Cárdenas
Fecha de Creación: 2025-05-10
Última Modificación: 2025-05-15
Versión: 2.0.0

Descripción:
Este script forma parte de la capa DIAMOND del proyecto BigData_Project.
Lee el archivo refinado desde MinIO (dev-diamond), infiere el esquema con Spark,
crea la tabla si no existe en PostgreSQL y realiza la inserción con COPY. Finalmente,
hace un respaldo del archivo en el bucket dev-gold.

Dependencias:
- Python >= 3.8
- Librerías: pyspark, boto3, dotenv, psycopg2, logging
"""

# %%
# ================================================================================
# PASO 1: IMPORTACIÓN DE LIBRERÍAS
# ================================================================================
import os
import logging
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv
import boto3
import psycopg2
from pyspark.sql import SparkSession

# %%
# ================================================================================
# PASO 2: CARGA DE VARIABLES Y CONFIGURACIÓN
# ================================================================================
load_dotenv("/home/jovyan/.env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BUCKET_DIAMOND    = os.getenv("MINIO_BUCKET_DIAMOND")
BUCKET_GOLD       = os.getenv("MINIO_BUCKET_GOLD")
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD")

POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB       = os.getenv("POSTGRES_DB")
HOST_POSTGRES     = os.getenv("HOST_POSTGRES_NBK")
PORT_POSTGRES     = os.getenv("PORT_POSTGRES")


# %%
# ================================================================================
# PASO 3: CREAR SPARKSESSION CON CONEXIÓN S3A
# ================================================================================
spark = SparkSession.builder \
    .appName("Carga DIAMOND a PostgreSQL") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", "/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.901.jar") \
    .getOrCreate()

# %%
# ================================================================================
# PASO 4: DETECTAR ARCHIVO MÁS RECIENTE EN dev-diamond
# ================================================================================
s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                  aws_access_key_id=MINIO_ACCESS_KEY,
                  aws_secret_access_key=MINIO_SECRET_KEY)

today = datetime.now().strftime("%Y%m%d")
prefix = "LOCAL_PACIENTES/"
response = s3.list_objects_v2(Bucket=BUCKET_DIAMOND, Prefix=prefix)

archivos = sorted(
    [obj["Key"] for obj in response.get("Contents", []) if today in obj["Key"]],
    reverse=True
)

if not archivos:
    raise FileNotFoundError("No se encontró ningún archivo Parquet para hoy en DIAMOND")

key_parquet = archivos[0]
ruta_s3a_parquet = f"s3a://{BUCKET_DIAMOND}/{key_parquet}"
logging.info(f"Archivo parquet encontrado: {ruta_s3a_parquet}")

# %%
# ================================================================================
# PASO 5: LECTURA CON SPARK Y GENERACIÓN DE SCHEMA
# ================================================================================
df_diamond = spark.read.parquet(ruta_s3a_parquet)
schema = df_diamond.schema

type_mapping = {
    "StringType": "TEXT", "IntegerType": "INTEGER", "LongType": "BIGINT",
    "ShortType": "SMALLINT", "DoubleType": "DOUBLE PRECISION", "FloatType": "REAL",
    "BooleanType": "BOOLEAN", "DateType": "DATE", "TimestampType": "TIMESTAMP", "DecimalType": "NUMERIC"
}

columnas_sql = [
    f"{field.name} {type_mapping.get(type(field.dataType).__name__, 'TEXT')}"
    for field in schema.fields
]

nombre_tabla = "pacientes_diamond"


# %%
# ================================================================================
# PASO 6: CREAR TABLA EN POSTGRESQL Y COPIAR DATOS
# ================================================================================
conn = psycopg2.connect(
    host=HOST_POSTGRES,
    port=PORT_POSTGRES,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cur = conn.cursor()

cur.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = %s
    );
""", (nombre_tabla,))
existe = cur.fetchone()[0]

if not existe:
    columnas_creacion = ",\n    ".join(columnas_sql)
    cur.execute(f"CREATE TABLE {nombre_tabla} (\n    {columnas_creacion}\n);")
    conn.commit()
    logging.info(f"Tabla '{nombre_tabla}' creada correctamente.")

    # Guardar como CSV temporal
    ruta_csv = f"/tmp/{nombre_tabla}.csv"
    df_diamond.repartition(1).write.mode("overwrite").option("header", True).csv(f"file:///tmp/{nombre_tabla}_dir")

    # Buscar el archivo CSV dentro del folder generado
    archivo_csv = next(
        f for f in os.listdir(f"/tmp/{nombre_tabla}_dir") if f.endswith(".csv")
    )
    with open(f"/tmp/{nombre_tabla}_dir/{archivo_csv}", "r") as f:
        cur.copy_expert(f"COPY {nombre_tabla} FROM STDIN WITH CSV HEADER", f)
    conn.commit()
    logging.info(f"Datos insertados en '{nombre_tabla}' con COPY.")
else:
    logging.info(f"La tabla '{nombre_tabla}' ya existe. No se insertarán datos.")

cur.close()
conn.close()


# %%
# ================================================================================
# PASO 7: BACKUP EN MinIO - BUCKET dev-gold
# ================================================================================
timestamp = datetime.now().strftime("%Y%m%d%H%M")
ruta_backup_gold = f"LOCAL_PACIENTES/{nombre_tabla}_{timestamp}.parquet"

df_diamond.repartition(1).write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/{ruta_backup_gold}")
logging.info(f"Backup subido a s3://{BUCKET_GOLD}/{ruta_backup_gold}")


