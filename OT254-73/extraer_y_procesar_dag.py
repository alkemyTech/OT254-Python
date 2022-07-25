from asyncio import tasks
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from decouple import config


def minuscula_y_sin_guion(string):
    """
    recive un string
    remplaza los guiones por espacios y
    finalmente lo devuelve lowercase
    """
    valor = string.lower()
    valor = valor.replace("-"," ")
    return valor

def male_or_female(gender):
    """
    recive un string
    conteniendo "M" o "F"
    y devuelve "Male" o "Female"
    dependiendo el caso
    """
    if gender == "M":
        return ("Male")
    elif gender == "F":
        return ("Female")

def extraer_moron():
    """
    extrae de la base de datos una tabla
    que obtiene ejecutando universidad_de_moron.sql

    retorna la tabla como json
    """
    #intento conectar a db 
    try:
        engine = create_engine(config('DB_DATA_CONNECT'))
        engine.connect()
    except:
        return "error al conectar con la base de datos"

    #Abro archivos con el path absoluto escrito en el .env
    file = open(config('UNIVERSIDAD_DE_MORON'),"r")
    moron_sql = file.read()

    #ejecuto el sql con pandas, obteniendo dataframes
    moron_df = pd.read_sql(moron_sql, config('DB_DATA_CONNECT'))

    #pusheo al xcom para que otra tarea pueda obtener este df
    return moron_df.to_json()
    


def extraer_rio_cuarto():
    """
    extrae de la base de datos una tabla
    que obtiene ejecutando universidad_de_rio_cuarto.sql

    retorna la tabla como json
    """
    #intento conectar a db 
    try:
        engine = create_engine(config('DB_DATA_CONNECT'))
        engine.connect()
    except:
        return "error al conectar con la base de datos"

    #Abro archivos con el path absoluto escrito en el .env
    file = open(config('UNIVERSIDAD_RIO_CUARTO'),'r' )
    rio_cuarto_sql = file.read()

    #ejecuto el sql con pandas, obteniendo dataframes
    rio_cuarto_df = pd.read_sql(rio_cuarto_sql, config('DB_DATA_CONNECT'))

    #pushiar xcom para que otra tarea pueda obtener este df
    return rio_cuarto_df.to_json()


def procesar_moron(ti):
    """
    une la tabla sql obtenida de la funcion
    "extraer_moron" la mergea con el archivo codigos_postales.csv
    y finalmente normaliza la tabla 
    """
    #temo los datos del xcom 
    m = ti.xcom_pull(task_ids="extraer_moron")
    moron_df = pd.DataFrame(eval(m))#el eval hace q pueda leer el dataframe convertido a json
    
    #mergeo tabla con archivo csv con codigos postales y localidades
    df_codigos_postales = pd.read_csv("/home/superalan/airflow/dags/codigos_postales.csv") 
    df_codigos_postales.rename(columns={'codigo_postal' : 'postal_code'}, inplace= True)
    moron_df=moron_df.merge(df_codigos_postales, on="postal_code",how='left')

    #proceso los datos restantes
    moron_df.rename(columns={'localidad' : 'location'}, inplace= True)
    moron_df['location'] = moron_df['location'].apply(minuscula_y_sin_guion)
    moron_df['career'] = moron_df['career'].apply(minuscula_y_sin_guion) 
    moron_df['university'] = moron_df['university'].apply(minuscula_y_sin_guion) 
    moron_df['postal_code'] = moron_df['postal_code'] = moron_df['postal_code'].apply(str)
    moron_df.rename(columns= {'edad' : 'age'}, inplace= True)
    moron_df['first_name'] = moron_df['first_name'].apply(minuscula_y_sin_guion)
    moron_df['last_name'] = moron_df['last_name'].apply(minuscula_y_sin_guion)
    #moron_df['inscription_date'] = pd.to_datetime(moron_df['inscription_date'], dayfirst=True)
    #moron_df['inscription_date'] =  pd.to_datetime(moron_df['inscription_date'], format='%d\/%m\/%Y')
    moron_df['gender'] = moron_df['gender'].apply(male_or_female)

    print(moron_df)
    
    
def procesar_rio_cuarto(ti):
    """
    une la tabla sql obtenida de la funcion
    "extraer_rio_cuarto" la mergea con el archivo codigos_postales.csv
    y finalmente normaliza la tabla
    """
    #temo los datos del xcom 
    r = ti.xcom_pull(task_ids="extraer_rio_cuarto")
    rio_df = pd.DataFrame(eval(r))#el eval hace q pueda leer el dataframe convertido a json

    #mergeo tabla con archivo csv con codigos postales y localidades
    df_codigos_postales = pd.read_csv("/home/superalan/airflow/dags/codigos_postales.csv") 
    df_codigos_postales.rename(columns={'codigo_postal' : 'postal_code'}, inplace= True)
    df_codigos_postales['localidad'] = df_codigos_postales['localidad'].apply(minuscula_y_sin_guion)
    rio_df['localidad'] = rio_df['localidad'].apply(minuscula_y_sin_guion)
    rio_df = rio_df.merge(df_codigos_postales, on='localidad',how='left')

    #proceso los datos restantes
    rio_df.rename(columns={'localidad' : 'location'}, inplace= True)
    rio_df['career'] = rio_df['career'].apply(minuscula_y_sin_guion) 
    rio_df['university'] = rio_df['university'].apply(minuscula_y_sin_guion)
    rio_df['edad'] = rio_df['edad'] % 100
    rio_df.rename(columns= {'edad' : 'age'}, inplace= True)
    rio_df['postal_code'] = rio_df['postal_code'].apply(str)
    rio_df['first_name'] = rio_df['first_name'].apply(minuscula_y_sin_guion)
    rio_df['last_name'] = rio_df['last_name'].apply(minuscula_y_sin_guion)
    #rio_df['inscription_date'] = pd.to_datetime(rio_df['inscription_date'])
    #rio_df['inscription_date'] =  pd.to_datetime(rio_df['inscription_date'], format='%Y\/%b\/%d')
    rio_df['gender'] = rio_df['gender'].apply(male_or_female)
    
    print(rio_df)

    

default_args = {
    'retries':1,
    'retry_delay': timedelta(seconds=2)
}

#se definen las propiedades del dag.
with DAG(
    'OT254_73',
    description="con archivos .sql extrae y procesa tablas de la db",
    schedule_interval=timedelta(days = 1 ),
    start_date=datetime(2022, 7, 20),
    ) as dag:
    

    extraer__rio_cuarto = PythonOperator(
        task_id = 'extraer_rio_cuarto',
        python_callable = extraer_rio_cuarto,
    )
    extraer__moron = PythonOperator(
        task_id = 'extraer_moron',
        python_callable = extraer_moron,
    )
    procesar__rio_cuarto = PythonOperator(
        task_id = 'procesar_rio_cuarto',
        python_callable = procesar_rio_cuarto,
    )
    procesar__moron = PythonOperator(
        task_id = 'procesar_moron',
        python_callable = procesar_moron,
    )
    
extraer__moron >> procesar__moron
extraer__rio_cuarto >> procesar__rio_cuarto