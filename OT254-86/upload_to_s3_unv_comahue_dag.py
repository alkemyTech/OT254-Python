'''
    se crea un dag para subir el archivo .txt con los datos de la univ de comahue a s3 utilizando 
    un operator echo por la comunidad.

'''

from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

with DAG(
    dag_id='upload_to_s3_unv_comahue_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 7, 20),
    catchup=False
) as dag: 
    task_upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={
        'filename': './comahue.txt',
        'key': 'comahue.txt',
        'bucket_name': 'bds-airflow-bucket'
    }
)