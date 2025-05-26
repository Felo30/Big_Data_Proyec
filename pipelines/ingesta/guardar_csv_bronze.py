# %%
"""
================================================================================
Nombre del Script: guardar_csv_bronze.py
Autor: Félix Cárdenas
Fecha de Creación: 2025-05-08
Última Modificación: 2025-05-21
Versión: 2.1.0

Descripción:
Este script forma parte de la capa BRONZE del proyecto BigData_Project.
Lee un archivo CSV crudo con Spark y lo guarda directamente en el bucket
`dev-bronze` de MinIO usando `s3a://`, sin pasar por pandas ni boto3.

Puede ejecutarse directamente (modo script/notebook) o como `python_callable`
en un DAG de Airflow.

Dependencias:
- Python >= 3.8
- pyspark, python-dotenv
================================================================================
"""

# PASO 1: IMPORTACIONES
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# PASO 1.5: Configurar entorno para Spark (antes de importar pyspark)
os.environ["SPARK_HOME"] = "/opt/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["PYTHONPATH"] = "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

# PASO 1.6: Recién ahora importar Spark
from pyspark.sql import SparkSession


# PASO 2: FUNCIÓN PRINCIPAL
def run_guardar_csv_bronze():
    load_dotenv("/opt/airflow/.env")

    ruta_csv_local = "/opt/airflow/project/datos/csv/pacientes_crudo.csv"
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

    spark = SparkSession.builder \
        .appName("Guardar CSV en MinIO con Spark") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars", "/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.901.jar") \
        .getOrCreate()

    df = spark.read.option("header", True).csv(ruta_csv_local)
    df.repartition(1).write.mode("overwrite").option("header", True).csv(output_path)

    print(f"✅ CSV guardado correctamente en: {output_path}")

# PASO 3: LLAMADO DIRECTO DESDE SCRIPT
if __name__ == "__main__":
    run_guardar_csv_bronze()
