from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from src.uf_functions import df_csv, df_txt

default_args = {
    "owner": "alkemy",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2022, 7, 1),
    "email": ["jmsiro@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    'retry_delay': timedelta(minutes=2),
    # 'on_retry_callback': , # Add logging call
    # 'on_success_callback': 
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