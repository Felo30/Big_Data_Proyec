# %%
"""
================================================================================
Nombre del Script: cargar_pacientes_diamond.py
Autor: Félix Cárdenas
Fecha de Creación: 2025-05-10
Última Modificación: 2025-05-10
Versión: 1.0.0

Descripción:
Este script forma parte de la capa DIAMOND del proyecto BigData_Project.
Se encarga de tomar los datos refinados desde el bucket dev-silver en formato Parquet,
leerlos con Spark, inferir el schema y tipos de datos, y generar automáticamente
la tabla correspondiente en PostgreSQL si no existe. Luego realiza la carga de datos
mediante COPY desde un archivo CSV temporal. Finalmente, genera un backup en el bucket
dev-diamond con timestamp.

Pasos principales:
1. Lectura desde MinIO (SILVER) con boto3.
2. Procesamiento con Spark para obtención de schema.
3. Creación de tabla en PostgreSQL si no existe.
4. Inserción eficiente con COPY (psycopg2).
5. Backup del dataset insertado en MinIO (DIAMOND).

Dependencias:
- Python >= 3.8
- Librerías: pyspark, pandas, boto3, python-dotenv, psycopg2, logging
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
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession

# %%
# ================================================================================
# PASO 2: CONFIGURACIÓN DE VARIABLES
# ================================================================================

load_dotenv("/home/jovyan/.env")  # Ruta de tu .env
# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Spark
spark = SparkSession.builder.appName("Capa DIAMOND").getOrCreate()

#buckets 
BUCKET_SILVER     = os.getenv("MINIO_BUCKET_SILVER")
BUCKET_DIAMOND    = os.getenv("MINIO_BUCKET_DIAMOND")
#Coneccion a minio
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD")
#coneccion a base de datos
POSTGRES_USER            = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD            = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB            = os.getenv("POSTGRES_DB")
HOST_POSTGRES            = os.getenv("HOST_POSTGRES_NBK")
PORT_POSTGRES            = os.getenv("PORT_POSTGRES") 

today = datetime.now().strftime("%Y%m%d")
dominio = "pacientes"
ruta_parquet = f"s3a://{BUCKET_SILVER}/LOCAL_{dominio.upper()}/pacientes_refinados_{today}*.parquet"


# %%
# ================================================================================
# PASO 3: DESCARGA DESDE MinIO CON BOTO3 Y LECTURA CON SPARK
# ================================================================================


# Configurar logging 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#Cliente Boto3 
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Buscar el archivo parquet más reciente del dia
today = datetime.now().strftime("%Y%m%d")
prefix = f"LOCAL_{dominio.upper()}/pacientes_refinados_{today}"

response = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefix)
archivos = sorted(
    [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")],
    reverse=True
)

if not archivos:
    raise FileNotFoundError(f"No se encontró ningún archivo Parquet con prefijo: {prefix}")

key_silver = archivos[0]
logging.info(f"Archivo Parquet encontrado: s3://{BUCKET_SILVER}/{key_silver}")

#Descargar el archivo a /tmp
ruta_local_parquet = f"/tmp/{os.path.basename(key_silver)}"
with open(ruta_local_parquet, "wb") as f:
    s3.download_fileobj(BUCKET_SILVER, key_silver, f)

#Leer con Spark desde disco local 
df_diamond = spark.read.parquet(f"file://{ruta_local_parquet}")
# Leer schema del archivo parquet ya cargado
schema = df_diamond.schema


# %%
# ================================================================================
# PASO 4: CREACIÓN DE DDL 
# ================================================================================
# Mapeo directo de tipos Spark a PostgreSQL
type_mapping = {
    "StringType": "TEXT", "IntegerType": "INTEGER", "LongType": "BIGINT",
    "ShortType": "SMALLINT", "DoubleType": "DOUBLE PRECISION", "FloatType": "REAL",
    "BooleanType": "BOOLEAN", "DateType": "DATE", "TimestampType": "TIMESTAMP","DecimalType": "NUMERIC"
}

# Construcción  de columnas
columnas_sql = [
    f"{field.name} {type_mapping.get(type(field.dataType).__name__, 'TEXT')}"
    for field in schema.fields
]

archivo = os.path.basename(key_silver)  
nombre_tabla = archivo.split("_")[0].lower() + "_diamond"

#nos conectamos a la base de datos
conn = psycopg2.connect(
    host=HOST_POSTGRES,
    port=PORT_POSTGRES,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

# Cursor ya conectado
cur = conn.cursor()



# %%
# ================================================================================
# PASO 5: CREACIÓN DE TABLA Y COPY EN POSTGRESQL
# ================================================================================

# Verificamos si la tabla ya existe
cur.execute(f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = %s
    );
""", (nombre_tabla,))
existe_tabla = cur.fetchone()[0]

if not existe_tabla:
    # Crear la tabla si no existe
    columnas_creacion = ",\n    ".join(columnas_sql)
    create_table_sql = f"CREATE TABLE {nombre_tabla} (\n    {columnas_creacion}\n);"
    cur.execute(create_table_sql)
    conn.commit()
    logging.info(f"Tabla '{nombre_tabla}' creada correctamente.")

    # Guardar el DataFrame en CSV temporal para COPY
    ruta_csv = f"/tmp/{nombre_tabla}.csv"
    df_diamond.toPandas().to_csv(ruta_csv, index=False)

    # Insertar con COPY
    with open(ruta_csv, "r") as f:
        cur.copy_expert(f"COPY {nombre_tabla} FROM STDIN WITH CSV HEADER", f)
    conn.commit()
    logging.info(f"Datos insertados correctamente en la tabla '{nombre_tabla}'.")
else:
    logging.info(f"La tabla '{nombre_tabla}' ya existe. No se realizará la creación ni la inserción.")

# Cerrar conexión
cur.close()
conn.close()


# %%
# ================================================================================
# PASO 6 : RESPALDO EN MinIO - BUCKET DIAMOND
# ================================================================================
# Crear nombre y ruta para el backup
timestamp = datetime.now().strftime("%Y%m%d%H%M")
backup_key = f"diamond/{nombre_tabla}_{timestamp}.csv"

buffer = BytesIO()
df_diamond.toPandas().to_csv(buffer, index=False)
buffer.seek(0)

s3.upload_fileobj(buffer, BUCKET_DIAMOND, backup_key)

logging.info(f"Backup subido a s3://{BUCKET_DIAMOND}/{backup_key}")


