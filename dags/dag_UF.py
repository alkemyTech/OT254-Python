from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from src.uf_functions import df_csv, df_txt
import logging
import logging.config

logging.config.fileConfig('/home/jmsiro/airflow/log.cfg',)
logger = logging.getLogger(__name__)

logger.debug('Initiating DAG_uni_flores')

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
    'DAG_uni_flores',
    default_args = default_args,
    description = '',
    schedule_interval = '@daily',
) as dag:

    export_csv = PythonOperator(
        dag = dag,
        task_id = "export_csv_uf",
        python_callable = df_csv,
        op_kwargs = {
            'path_query_UF': '/home/jmsiro/airflow/dags/src/query_UF.sql',
            'path_PC': '/home/jmsiro/airflow/dags/data_base/codigos_postales.csv',
            'path_csv': '/home/jmsiro/airflow/dags/data_base', 
            'filename': 'UF'}
    )
    
    export_txt = PythonOperator(
        dag = dag,
        task_id = "export_txt_uf",
        python_callable = df_txt,
        op_kwargs = {
            'path_txt': '/home/jmsiro/airflow/dags/data_processed', 
            'filename': 'UF'}
    )  #To do, options: a) use varibles from airflow: {{ var.value.name_of_var}} b) use json variable from airflow Variable.get("json_variabale", deserialize_jason=True)

    export_csv >> export_txt