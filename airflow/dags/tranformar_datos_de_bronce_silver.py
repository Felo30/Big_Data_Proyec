from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Agregamos el path base del proyecto
sys.path.append("/opt/airflow/project")

# Importamos correctamente la función desde el módulo transformacion
from transformacion.transformar_datos_silver import run_refinar_csv_silver

default_args = {
    "owner": "felix",
    "start_date": datetime(2025, 5, 26),
    "retries": 1
}

with DAG(
    dag_id="dag_refinar_csv_silver",
    default_args=default_args,
    description="Refina los datos crudos del BRONZE y guarda el archivo limpio en SILVER",
    schedule_interval=None,
    catchup=False,
    tags=["silver", "minio", "spark"]
) as dag:

    refinar_csv = PythonOperator(
        task_id="refinar_csv_en_silver",
        python_callable=run_refinar_csv_silver,
    )

    refinar_csv
