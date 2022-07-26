from sqlalchemy import text
import pandas as pd
import src.engine
import os
import logging
import logging.config

logging.config.fileConfig('/home/jmsiro/airflow/log.cfg',)
logger = logging.getLogger(__name__)

global column_list 
column_list= ['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email']

def dataf_uvm (path_query_UVM, path_PC):
    """Connects to the database, execute the query for Universidad de Villa Maria (.slq).

    Args:
        path_query_UVM (str): path to .sql file
        path_PC (str): path to .csv file

    Returns:
        dataframe: Result of the query made to the database
    """
    conn = src.engine.eng_con()

    with open(path_query_UVM, 'r') as sql_statement:
        sql_statement2 = text(sql_statement.read())

    df_UVM = pd.read_sql_query(sql_statement2,con=conn)
    df_UVM['university'] = df_UVM['university'].str.replace('_', ' ')
    df_UVM['career'] = df_UVM['career'].str.replace('_', ' ')
    # Replace the underscores that separate words in 'location' with blank spaces to make possible the merging
    df_UVM['location'] = df_UVM['location'].str.replace('_', ' ')
        
    df_PC = pd.read_csv(path_PC)
    df_PC.columns = ['postal_code', 'location']

    df = pd.merge(
        df_UVM,
        df_PC,
        on='location',
        how = 'left'
    )

    # Set order of the data frame columns
    df = df[column_list]

    return df

def df_csv(path_query_UVM, path_PC, path_csv, filename, ti):
    """
    Args:
        path_query_UVM (str): path to .sql file
        path_PC (str): path to .csv file
        path_csv (str): path to save the file with the query result
        filename (str): name for the file

    Returns:
        file: CSV file containing the result of the query made to the database
    """
    df = dataf_uvm (path_query_UVM, path_PC)
    
    path_ = os.path.join(path_csv, filename + '.csv')
    
    if not os.path.isdir(path_csv):
        os.mkdir(path_csv)
        
    df.to_csv(path_, index=False)
    
    ti.xcom_push(key='csv_file_uvm', value=path_)

def df_txt(path_txt, filename, ti):
    """Takes the .csv and normalize it into a .txt file

    Args:
        path_txt (str): path to save the file with the normalized data
        filename (str): mame for the file
    
    Returns:
        dataframe: Result of the query made to the database
    """
    df = pd.read_csv(ti.xcom_pull(key='csv_file_uvm', task_ids='export_csv_uvm'))
    
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
        

