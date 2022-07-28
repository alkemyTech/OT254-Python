from sqlalchemy import text
import pandas as pd
import src.engine
import os
import logging
import logging.config
import boto3

logging.config.fileConfig('/home/jmsiro/airflow/log.cfg')
logger = logging.getLogger(__name__)

global column_list 
column_list= ['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email']

def dataf_uf (path_query_UF, path_PC):
    """Connects to the database, execute the query for Universidad de Flores (.slq).

    Args:
        path_query_UF (str): path to .sql file
        path_PC (str): path to .csv file

    Returns:
        dataframe: Result of the query made to the database
    """
    conn = src.engine.eng_con()

    with open(path_query_UF, 'r') as sql_statement:
        sql_statement2 = text(sql_statement.read())

    df_UF = pd.read_sql_query(sql_statement2,con=conn)
    # Convert the 'postal_code' from type object to integer to make possible the merging
    df_UF['postal_code'] = df_UF['postal_code'].astype(int)

    df_PC = pd.read_csv(path_PC)
    df_PC.columns = ['postal_code', 'location']

    df = pd.merge(
        df_UF,
        df_PC,
        on = 'postal_code',
        how = 'left',
    )
    
    # Set order of the data frame columns
    column_list = ['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email']
    df = df[column_list]
    logger.info("Query made succesfuly!")
    return df
    
def df_csv(path_query_UF, path_PC, path_csv, filename, ti):
    """
    Args:
        path_query_UVM (str): path to .sql file
        path_PC (str): path to .csv file
        path_csv (str): path to save the file with the query result
        filename (str): name for the file

    Returns:
        file: CSV file containing the result of the query made to the database
    """
    df = dataf_uf (path_query_UF, path_PC)
    
    path_ = os.path.join(path_csv, filename + '.csv')
    
    if not os.path.isdir(path_csv):
        os.mkdir(path_csv)
        
    df.to_csv(path_, index=False)
    
    logger.info("File {} created!".format(os.path.basename(path_)))
    
    ti.xcom_push(key='csv_file_uf', value=path_)

def df_txt(path_txt, filename, ti):
    """Takes the .csv and normalize it into a .txt file

    Args:
        path_txt (str): path to save the file with the normalized data
        filename (str): mame for the file
    
    Returns:
        dataframe: Result of the query made to the database
    """
    df = pd.read_csv(ti.xcom_pull(key='csv_file_uf', task_ids='export_csv_uf'))
    
    path_ = os.path.join(path_txt, filename + '.txt')
    
    if not os.path.isdir(path_txt):
        os.mkdir(path_txt)
    
    formats = {'university': 'str',
               'career': 'str',
               'inscription_date': 'str',
               'first_name': 'str',
               'last_name': 'str',
               'gender': 'str',
               'age': 'float',
               'postal_code': 'str',
               'location': 'str' ,
               'email': 'str'}
    
    df['gender'] = df['gender'].str.replace('M', 'male').replace('M', 'female')

    for col in column_list:
        df.astype({col : formats[col]}).dtypes
    
    df= df.applymap(lambda c: c.lower() if type(c) == str else c)
    df.to_csv(path_, sep=',', index=False)
    
    logger.info("File {} created!".format(os.path.basename(path_)))
    
    ti.xcom_push(key='txt_file_uf', value=path_)

def upload_to_s3(ti, bucket_name, aws_access_key_id, aws_secret_access_key, region_name):
    """ Upload a file to a AWS S3 bucket
    
    Args:
        file_path (str): Path to the file
        bucket_name (str): Name of the AWS S3 bucket
        aws_access_key_id (str): AWS Public Key
        aws_secret_access_key (str): AES Secret Key
        region_name (str): AWS Region
    """
    # Creates connection
    s3_client = boto3.client(service_name='s3',region_name=region_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    # Gets file name
    file_path = ti.xcom_pull(key='txt_file_uf', task_ids='export_txt_uf')
    
    file_name = os.path.basename(str(file_path))

    try:
        # Uploads file if it doesn't exist or replace the old one
        s3_client.upload_file(file_path, bucket_name, file_name)
        logger.info("File {}, uploaded succesfully to bucket {}".format(file_name, bucket_name))
        
    except Exception as e:
        logger.error(e)
