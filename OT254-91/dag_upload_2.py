from airflow.models.dag import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging

Bucket_Name='cohorte-julio-8972766c'
filename='Universidad Nacional de Tres de Febrero.txt'
file_path='/home/andresgb/Documentos/Andres/OT254-Python/OT254-91/'+filename


def upload_file(**context):
    hook = S3Hook('con_s3')
    hook.get_conn()
    #Existe el archivo?
    existe = hook.check_for_key(key=context['key'], bucket_name=context['bucket_name'])
    if existe:
        logging.info('El archivo ya existe en el bucket')
    else:
        logging.info('Subiendo el archivo')
        hook.load_file(filename=context['file_path'], key=context['key'], bucket_name=context['bucket_name'])
        logging.info('Archivo subido')

with DAG(
    dag_id='dag_upload_2',
    description='ejecutar queries sqls de dos universidades',
    schedule_interval=timedelta(days = 1),
    start_date=datetime(2022, 7, 22),
    ) as dag:

    task1 = PythonOperator(
        task_id = 'ot254_90_upload_file',
        python_callable = upload_file,
        op_kwargs={
            'file_path': file_path,
            'key': filename,
            'bucket_name': Bucket_Name,
        },
    )
    task1
