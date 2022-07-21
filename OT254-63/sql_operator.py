#Implementar SQL Operator 
from airflow import DAG
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from decouple import config
import logging
import json
import pandas as pd
from airflow.operators.python_operator import PythonOperator



def tomar_daros_sql_1():
    #Abrir archivo con el query1
    with open(config('QUERY1_FILE'), 'r') as f:
        query = f.read()

    #Utilizar pandas para ejecutar el query
    df = pd.read_sql(query, config('CONNECTION_DB'))
    return df.to_json()

def tomar_daros_sql_2():
    #Abrir archivo con el query1
    with open(config('QUERY2_FILE'), 'r') as f:
        query = f.read()
    

    #Utilizar pandas para ejecutar el query
    df = pd.read_sql(query, config('CONNECTION_DB'))
    return df.to_json()

with DAG(
    'sql_operator',
    description='Setting up a DAG to load data from SQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,7,18)
) as  DAG_SQL:
    logging.info('Starting the DAG')
    python_operator1 = PythonOperator(
        task_id='tomar_dados_sql_1',
        python_callable=tomar_daros_sql_1
    )
    python_operator2 = PythonOperator(
        task_id='tomar_dados_sql_2',
        python_callable=tomar_daros_sql_2
    )
    [python_operator1, python_operator2]
    logging.info('Finished the DAG')