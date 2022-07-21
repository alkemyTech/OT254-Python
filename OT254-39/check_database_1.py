from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import exc, create_engine, inspect
from decouple import config
from retry import retry


@retry(delay=10, tries=3)
def test():
    try:
        engine = create_engine(config('DB_URL'), echo=True)
        con = engine
        inspector = inspect(con)
        if inspector.get_table_names().__contains__('palermo_tres_de_febrero') and inspector.get_table_names().__contains__('jujuy_utn'):
            print("Tables exist")
        else:
            print("Tables don't exist")
    except exc.OperationalError as e:
        print("Error: ", e)
        print("Error connecting to database")
    
# se definen las propiedades del dag.
with DAG(
    'check_database_1',
    description='comprobar la conexion con la db.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 15),
) as dag:

    task_check_database = PythonOperator(
        task_id='check_database',
        python_callable=test
    )
