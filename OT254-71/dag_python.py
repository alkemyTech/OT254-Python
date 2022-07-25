from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


from script import solucion_universidad_tres_de_febrero, solucion_universidad_tecnologica_nacional

with DAG(
    'dag_python',
    description='Setting up a dag implements PythonOperator to run two functions.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,7,18)
    ) as  DAG_SQL:
    python_operator1 = PythonOperator(
        task_id='tomar_dados_sql_1',
        python_callable=solucion_universidad_tres_de_febrero
    ),
    python_operator2 = PythonOperator(
        task_id='tomar_dados_sql_2',
        python_callable=solucion_universidad_tecnologica_nacional
    )