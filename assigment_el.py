import yaml
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from resources.scripts.retail import extract_logic, load_logic, update_ingest_type_logic

# Load konfigurasi dari file YAML
with open("dags/resources/config/retail.yaml", "r") as f:
    config = yaml.safe_load(f)


# Default arguments for the DAG
default_args = {
    'retries': 3,
    'retry_delay': 30
}

@dag(
    dag_id='biweekly_friday_job',
    default_args=default_args,
    description='Run job every 2 hours on 1st and 3rd Friday of the month from 9 AM to 9 PM',
    schedule_interval='15 9-21/2 * * 5#1,5#3',
    start_date = datetime(2024, 10, 18),
    catchup=False,
    tags=['assigment']
)

def biweekly_friday_job():
    start_task          = EmptyOperator(task_id="start_task")
    end_task            = EmptyOperator(task_id="end_task")

    # ========== Membuat task EL (Extract & Load) secara dinamis berdasarkan konfigurasi ==========
    for item in config.get("ingestion", []):

        # --- Ekstrak data dari mysql ke staging area ---
        @task(task_id=f"extract_to_staging_area.{item['table']}")
        def extract(item, **kwargs):
            extract_logic(item, **kwargs)

        # --- Load data dari staging area ke postgres ---
        @task(task_id=f"load_to_bronze.{item['table']}")
        def load(item, **kwargs):
            load_logic(item, **kwargs)


        # --- Update status ingest type di airflow variable ---
        @task(task_id=f"update_ingest_type.{item['table']}")
        def update_ingest_type(item, **kwargs):
            update_ingest_type_logic(item, **kwargs)


        start_task >> extract(item) >> load(item) >> update_ingest_type(item) >> end_task


# Mendefinisikan DAG
biweekly_friday_job()