import random
import datetime as dt
import csv
import psycopg2

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

CSV_PATH = '/opt/airflow/dags/csv/temp_data.csv'
SQL_DIR_PATH = './sql'

default_args = {
    'owner': 'airflow',
    'tags': ['work_task'],
    'start_date': dt.datetime(2024, 11, 21),
    'retries': 5,
}

first_names = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer",
    "Michael", "Linda", "William", "Elizabeth", "David", "Barbara",
    "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
    "Charles", "Karen"
]

with DAG(
        dag_id='work_task',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        sql=f'{SQL_DIR_PATH}/create_table.sql',
        postgres_conn_id='postgres_default',
    )


    @task
    def gen_id_name() -> tuple[int, str]:
        id_pers = random.randint(1, 100)
        name = random.choice(first_names)
        return id_pers, name


    @task
    def save_to_csv(id_name: tuple[int, str]) -> None:
        with open(CSV_PATH, 'a', newline='') as f:
            writer = csv.writer(f)
            cur_date = dt.date.today()
            id_pers, name = id_name
            writer.writerow([id_pers, name, cur_date])


    def insert_data_to_postgres():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        with open(CSV_PATH, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                id_pers, name, cur_date = row
                cursor.execute(
                    """
                    INSERT INTO random_data (id, name, date)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (date) DO UPDATE
                    SET name = excluded.name, id = excluded.id;
                    """,
                    (id_pers, name, cur_date)
                )
        conn.commit()
        cursor.close()
        conn.close()


    save_to_postgres = PythonOperator(
        task_id='save_to_postgres',
        python_callable=insert_data_to_postgres
    )

    id_name = gen_id_name()

    create_table >> save_to_csv(id_name) >> save_to_postgres

