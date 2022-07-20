from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from decouple import config


def check_db():
    #intento conectar a db 
    try:
        engine = create_engine(config('DB_DATA_CONNECT'))
        engine.connect()
    except:
        return "error al conectar con la base de datos"
        
    #ejecuto tareas de universidades pedidas (OT254-17)

    #Abro archivos con el path absoluto escrito en el .env
    file = open(config('UNIVERSIDAD_DE_MORON'),"r")
    moron_sql = file.read()

    file = open(config('UNIVERSIDAD_RIO_CUARTO'),'r' )
    rio_cuarto_sql = file.read()

    df_codigos_postales = pd.read_csv(config('CODIGOS_POSTALES')) 

    #renombro la columna para poder mergiar
    df_codigos_postales.rename(columns={'codigo_postal' : 'postal_code'}, inplace= True)

    #ejecuto el sql con pandas, obteniendo dataframes
    moron_df = pd.read_sql(moron_sql, config('DB_DATA_CONNECT'))
    rio_cuarto_df = pd.read_sql(rio_cuarto_sql, config('DB_DATA_CONNECT'))

    #merge
    moron_df.merge(df_codigos_postales, on="postal_code",how='left')

    rio_cuarto_df['edad'] = rio_cuarto_df['edad'] % 100
    rio_cuarto_df.merge(df_codigos_postales, on='localidad',how='left')

    print(rio_cuarto_df)
    print("---")
    print(moron_df)

default_args = {
    'retries':5,
    'retry_delay': timedelta(seconds=5)
}

#se definen las propiedades del dag.
with DAG(
    'ot254_41',
    description='comprobar la conexion con la db.',
    schedule_interval=timedelta(days = 1 ),
    start_date=datetime(2022, 7, 20),
    ) as dag:
    

    ot254_41 = PythonOperator(
        task_id = 'ot254_41',
        python_callable = check_db,
    )
    
ot254_41 