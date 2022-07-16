import pandas as pd 
from decouple import config


def procesar_datos():
        #se lee el archivo sql correspondiente a la univ. nacional del comahue
        file = open('Univ_Nacional_Del_Comahue.sql','r' )
        univ_nacional_del_comahue_sql = file.read()

        #se lee el archivo sql correspondiente a la univ. del salvador
        file = open('Universidad_Del_Salvador.sql','r' )
        universidad_del_salvador_sql = file.read()

        #Se ejecutan laa consultas sql correspondientes a cada universidad y se genera un dataframe para cada una.
        df_unv_Comahue = pd.read_sql(univ_nacional_del_comahue_sql, config('DB_DATA_CONNECT'))
        df_unv_del_salvador = pd.read_sql(universidad_del_salvador_sql, config('DB_DATA_CONNECT'))