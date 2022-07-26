"""
[OT254-75] H group, Sprint 1
Configures a Python Operator to execute the function that processes the data
from Universidad del Cine and Universidad de Buenos Aires
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from pathlib import Path
from data_processing import data_processing

path = Path(__file__).resolve().parent.parent


DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=60)
}

with DAG(
        dag_id='data_processing',
        description="""
                Executes the function that manages 
                data processing
                """,
        default_args=DEFAULT_ARGS,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 18)
) as dag:
    data_processing_task = PythonOperator(
        task_id='data_processing',
        python_callable=data_processing,
        op_args=[str(path) + '/files/cine_univ.csv',
                 str(path) + '/files/ba_univ.csv',
                 'https://drive.google.com/u/0/uc?id=1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ&export=download'
                 ]
    )

data_processing_task
