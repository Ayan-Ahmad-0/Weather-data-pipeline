# dags/weather_pipeline_dag.py
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

default_args = {
    "owner": "ayan",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="weather_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["weather", "portfolio"],
)
def weather_pipeline():

    @task
    def extract():
        from src.extract.fetch_weather import fetch_all_cities
        records = fetch_all_cities()
        # records must be JSON-serializable for XCom (they already are: str/float/dict)
        return records

    @task
    def load(records: list[dict]):
        from src.load.load_to_postgres import load_records
        inserted = load_records(records)
        return inserted

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    load(extract()) >> dbt_run >> dbt_test

weather_pipeline()