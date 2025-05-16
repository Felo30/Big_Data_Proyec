from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Agregás el path para importar correctamente
sys.path.append("/opt/airflow/project")

# Importás la función principal
from infraestructura.crear_buckets_minio import run_creacion_buckets

default_args = {
    "owner": "felix",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="dag_creacion_buckets_minio",
    default_args=default_args,
    description="Crea los buckets necesarios en MinIO para el Data Lake",
    schedule_interval=None,
    catchup=False,
    tags=["infraestructura", "minio", "data_lake"],
) as dag:

    crear_buckets = PythonOperator(
        task_id="crear_buckets_minio",
        python_callable=run_creacion_buckets
    )
