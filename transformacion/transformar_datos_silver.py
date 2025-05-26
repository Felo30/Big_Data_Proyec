# ==============================================================================
# Script: refinar_pacientes_silver.py
# Autor: Félix Cárdenas
# Versión: 2.2.0
# Descripción:
#   Refinamiento de datos crudos desde MinIO (BRONZE) y guardado en formato Parquet en SILVER.
# ==============================================================================

import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date
import boto3

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración para Spark (para ejecución desde Airflow o PythonOperator)
os.environ["SPARK_HOME"] = "/opt/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["PYTHONPATH"] = "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"


def run_refinar_csv_silver():
    try:
        # ----------------------------------------------------------------------
        # 1. Variables de entorno
        # ----------------------------------------------------------------------
        load_dotenv("/opt/airflow/.env")

        MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
        MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
        MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
        BUCKET_BRONZE    = os.getenv("MINIO_BUCKET_BRONZE")
        BUCKET_SILVER    = os.getenv("MINIO_BUCKET_SILVER")

        logger.info("Variables de entorno cargadas correctamente.")

        # ----------------------------------------------------------------------
        # 2. SparkSession
        # ----------------------------------------------------------------------
        try:
            spark = SparkSession.builder \
                .appName("Transformacion Silver") \
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
                .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.jars", "/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.901.jar") \
                .getOrCreate()
            logger.info("SparkSession creada correctamente.")
        except Exception as e:
            logger.error(f"Error al iniciar SparkSession: {e}")
            raise

        # ----------------------------------------------------------------------
        # 3. Detección del archivo más reciente en BRONZE
        # ----------------------------------------------------------------------
        try:
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
                raise FileNotFoundError(f"No se encontró archivo CSV para hoy: {today}")

            archivo_mas_reciente = sorted(archivos, reverse=True)[0]
            ruta_s3a = f"s3a://{BUCKET_BRONZE}/{archivo_mas_reciente}"
            logger.info(f"Archivo detectado: {ruta_s3a}")
        except Exception as e:
            logger.error(f"Error al detectar archivo en MinIO: {e}")
            raise

        # ----------------------------------------------------------------------
        # 4. Lógica de transformación
        # ----------------------------------------------------------------------
        try:
            df = spark.read.option("header", True).csv(ruta_s3a)
            df_limpio = df \
                .filter(col("nombre").isNotNull() & (col("nombre") != "")) \
                .filter((col("edad") > 0) & (col("edad") < 120)) \
                .filter(col("obra_social").isNotNull() & (col("obra_social") != "")) \
                .withColumn("fecha_turno", split(col("fecha_turno"), " ").getItem(0)) \
                .withColumn("fecha_turno", to_date("fecha_turno", "yyyy-MM-dd")) \
                .dropna(subset=["fecha_turno"]) \
                .dropDuplicates()
            logger.info("Transformación completada con éxito.")
        except Exception as e:
            logger.error(f"Error durante la transformación de datos: {e}")
            raise

        # ----------------------------------------------------------------------
        # 5. Guardado final en SILVER
        # ----------------------------------------------------------------------
        try:
            nombre_archivo = f"pacientes_refinados_{datetime.now().strftime('%Y%m%d%H%M')}.parquet"
            ruta_output = f"s3a://{BUCKET_SILVER}/LOCAL_PACIENTES/{nombre_archivo}"
            df_limpio.repartition(1).write.mode("overwrite").parquet(ruta_output)
            logger.info(f"Archivo guardado correctamente en SILVER: {ruta_output}")
        except Exception as e:
            logger.error(f"Error al guardar el archivo en SILVER: {e}")
            raise

    except Exception as final_error:
        logger.exception("Fallo crítico en el proceso de refinamiento de pacientes.")
        raise  # Para que Airflow lo marque como fallido


# Ejecución local (opcional)
if __name__ == "__main__":
    run_refinar_csv_silver()
