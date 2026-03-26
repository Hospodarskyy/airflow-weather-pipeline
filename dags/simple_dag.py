from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.python import PythonOperator


def _hello_world():
    logging.info("Hello, World!")


def _second_task():
    logging.info("second task")

with DAG(
    dag_id="first_dag",
    schedule="@daily",
    start_date=datetime.now() - timedelta(days=2),
    catchup=True,
    ) as dag:

    hello_world = PythonOperator(
        task_id="python_task",
        python_callable=_hello_world
    )

    second_task = PythonOperator(
        task_id="second_python_task",
        python_callable=_second_task
    )

    hello_world >> second_task