from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.bash_operator import BashOperator



#Formato del log: %Y-%m-%d - nombre_logger - mensaje
def config_logging():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO, filemode='a',datefmt='%Y-%m-%d')
    



with DAG(
    'log_dag',
    description='Setting up a DAG to log messages',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,7,18)
) as dag:
    config_logging()
    logging.info('Starting the DAG')
    bash_operator = BashOperator(
        task_id='log_message',
        bash_command='echo "Hello World"'
    )
    logging.info('Finished the DAG')
    
