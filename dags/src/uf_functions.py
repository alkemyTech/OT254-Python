from sqlalchemy import text
import pandas as pd
import src.engine
import os

global column_list 
column_list= ['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email']

def dataf_uf (path_query_UF, path_PC):
    
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

    return df
    
def df_csv(path_query_UF, path_PC, path_csv, filename, ti):
    
    df = dataf_uf (path_query_UF, path_PC)
    
    path_ = os.path.join(path_csv, filename + '.csv')
    
    if not os.path.isdir(path_csv):
        os.mkdir(path_csv)
        
    df.to_csv(path_, index=False)
    
    ti.xcom_push(key='csv_file_uf', value=path_)

def df_txt(path_txt, filename, ti):
    
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
        
