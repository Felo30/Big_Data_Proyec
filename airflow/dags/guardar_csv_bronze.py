from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Agregás el path al proyecto para que Airflow pueda encontrar tu script
sys.path.append("/opt/airflow/project")

# Importás tu función principal
from pipelines.ingesta.guardar_csv_bronze import run_guardar_csv_bronze

# Argumentos por defecto del DAG
default_args = {
    "owner": "felix",
    "start_date": datetime(2024, 1, 1),
}

# Definición del DAG
with DAG(
    dag_id="dag_guardar_csv_bronze",
    default_args=default_args,
    description="Guarda el CSV crudo en el bucket BRONZE de MinIO usando Spark",
    schedule_interval=None,  # lo ejecutás manualmente
    catchup=False,
    tags=["bronze", "minio", "spark"],
) as dag:

    guardar_csv = PythonOperator(
        task_id="guardar_csv_en_bronze",
        python_callable=run_guardar_csv_bronze,
    )

    guardar_csv
