from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from decouple import config
from src.uvm_functions import df_csv, df_txt, upload_to_s3
import logging
import logging.config

logging.config.fileConfig('/home/jmsiro/airflow/log.cfg')
logger = logging.getLogger(__name__)

logger.debug('Initiating DAG_uni_villa_maria')

def task_success(context):
    logger.info(f"DAG realizado con exito, id: {context['run_id']}")
def task_retry(context):
    logger.warning(f"El DAG ha fallado, {context['task_instance'].try_number}")
def task_failure(context):
    logger.exception(f"El DAG ha fallado, {context['task_instance_key_str']}")

default_args = {
    "owner": "alkemy",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2022, 7, 1),
    "retries": 5,
    'retry_delay': timedelta(minutes=2),
    'on_success_callback': task_success,
    'on_retry_callback': task_retry,
    'on_failure_callback': task_failure
}

with DAG(
    'DAG_uni_villa_maria',
    default_args = default_args,
    description = '',
    schedule_interval = '@daily',
) as dag:

    export_csv = PythonOperator(
        dag = dag,
        task_id = "export_csv_uvm",
        python_callable = df_csv,
        op_kwargs = {
            'path_query_UVM': '/home/jmsiro/airflow/dags/src/query_UVM.sql',
            'path_PC': '/home/jmsiro/airflow/dags/data_base/codigos_postales.csv',
            'path_csv': '/home/jmsiro/airflow/dags/data_base', 
            'filename': 'UVM'}
    )
    
    export_txt = PythonOperator(
        dag = dag,
        task_id = "export_txt_uvm",
        python_callable = df_txt,
        op_kwargs = {
            'path_txt': '/home/jmsiro/airflow/dags/data_processed', 
            'filename': 'UVM'}
    )  #To do, options: a) use varibles from airflow: {{ var.value.name_of_var}} b) use json variable from airflow Variable.get("json_variabale", deserialize_jason=True)
        
    upload_s3 = PythonOperator(
        dag = dag,
        task_id = "upload_to_s3_uvm",
        python_callable = upload_to_s3,
        op_kwargs = {
            'bucket_name': config('BUCKET_NAME'),
            'aws_access_key_id': config('PUBLIC_KEY'), 
            'aws_secret_access_key': config('SECRET_KEY'),
            'region_name': config('REGION')}
    ) 
    export_csv >> export_txt >> upload_s3
