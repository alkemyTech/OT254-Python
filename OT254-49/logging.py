from asyncio import tasks
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging


logging.basicConfig(
    filename ='logs.log',
    level = logging.INFO,
    format = '%(asctime)s - %(filename)s - %(levelname)s - %(message)s'
    )

default_args = {
    'retries':5,
    'retry_delay': timedelta(seconds=5)
}

#se definen las propiedades del dag.
with DAG(
    'ot254_49',
    description='configurar log',
    schedule_interval=timedelta(days = 1 ),
    start_date=datetime(2022, 7, 20),
    ) as dag:
    
    tarea = DummyOperator(
        task_id = 'tarea'
        )

tarea