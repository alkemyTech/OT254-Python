from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from decouple import config


def connection_check(bucket_name, file, key):
    hook = S3Hook('conns3')
    hook.load_file(bucket_name= bucket_name, key= key, filename= file, replace= True)


default_args = {
    'retries':1,
    'retry_delay': timedelta(seconds=3)
}

with DAG(
    'OT254-95_load_file_s3',
    description= 'Sube archivo txt a s3 bucket',
    default_args = default_args,
    schedule_interval= timedelta(days=1),
    start_date= datetime(2022, 7, 21),    
) as dag:

    load_s3 = PythonOperator(
        task_id="load_s3",
        python_callable= connection_check,
        op_kwargs={
            "key" : "universidad_rio_cuarto.txt",
            "file" : config('BASE')+"universidad_rio_cuarto.txt",
            'bucket_name': config('BUCKET')
        }
    )

load_s3