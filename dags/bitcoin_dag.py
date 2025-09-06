from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from utils.extract import run_extraction
from utils.verify_duckdb_data import verify_raw_table
from utils.visualize import generate_visualization


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "bitcoin_elt_pipeline",
    default_args=default_args,
    description="A simple ELT pipeline for Bitcoin data",
    start_date=datetime(2025, 9, 1),
    # Set schedule to none in order to trigger dag manually
    schedule=None,
    catchup=False,
)

extract_and_load = PythonOperator(
    task_id="extract_and_load",
    python_callable=run_extraction,
    dag=dag,
)

verify_data = PythonOperator(
    task_id="verify_data",
    python_callable=verify_raw_table,
    dag=dag,
)

transform = BashOperator(
    task_id="transform",
    bash_command="dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt/",
    dag=dag,
)

test = BashOperator(
    task_id="test",
    bash_command="dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt/",
    dag=dag,
)

visualize_data = PythonOperator(
    task_id="visualize_data",
    python_callable=generate_visualization,
    dag=dag,
)

extract_and_load >> verify_data >> transform >> test >> visualize_data
