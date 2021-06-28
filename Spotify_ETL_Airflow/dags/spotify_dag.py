from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.timezone import datetime
from spotify_etl import run_spotify_etl

with DAG('spotify_dag', start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False) as dag:

    run_etl = PythonOperator(task_id="complete_spotify_etl",
                             python_callable=run_spotify_etl)

