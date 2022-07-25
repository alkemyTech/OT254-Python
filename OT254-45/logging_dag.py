from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.dummy import DummyOperator
#se definen los parametro para la configuracion de los log
logging.basicConfig(filename='info.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%d-%m-%y', level=logging.INFO)


with DAG(
    'logging_dag',
    description='logging dag',
    schedule_interval=timedelta(days = 1 ),
    start_date=datetime(2022, 7, 15),
) as dag:
    task_1 = DummyOperator(task_id = 'tarea_1')





