import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator

from spark_process import spark_process
from dags.csv_to_sql import csv_to_sql

default_args = {
    'owner': 'airflow',
    'tags': ['project_dag'],
    'start_date': dt.datetime(2024, 10, 29),
    'retries': 5,
}

with DAG(
        dag_id='project_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:
    spark_process = PythonOperator(
         task_id='spark_process',
         python_callable=spark_process
    )

    csv_to_sql = PythonOperator(
        task_id='csv_to_sql',
        python_callable=csv_to_sql
    )

    spark_process >> csv_to_sql
