from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from procesar_datos import univ_del_salvador_data_process, univ_del_comahue_data_process


#se definen las propiedades del dag.
with DAG(
    'procesar_datos_dag',
    description='dag para procesar los datos de cada universidad',
    schedule_interval=timedelta(days = 1 ),
    start_date=datetime(2022, 7, 15),
    ) as dag:
    
    data_process_univ_del_salvador = PythonOperator(
        task_id = 'procesar datos univ del salvador',
        python_callable = univ_del_salvador_data_process
        )

    data_process_univ_comahue = PythonOperator(
        task_id = 'procesar datos univ. nac del comahue',
        python_callable = univ_del_comahue_data_process
        )




