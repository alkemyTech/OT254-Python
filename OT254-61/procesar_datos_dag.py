from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from procesar_datos import procesar_datos


#se definen las propiedades del dag.
with DAG(
    'procesar_datos',
    description='se procesarn los datos de cada universidad.',
    schedule_interval=timedelta(days = 1 ),
    start_date=datetime(2022, 7, 15),
    ) as dag:
    
    task_check_db_connection = PythonOperator(
        task_id = 'procesar_datos',
        python_callable = procesar_datos
        )




