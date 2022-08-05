
"""
toma los post de tipo Question con score de entre 100~200 y tambien toma las
respuestas devolviendo el tiempo promedio de respuesta 

"""
from functools import reduce
import pandas as pd
import numpy as np
import sys


def concat_df_answer_or_nan(df, df2):
    """
    Concatena los dos dataframes si no tienen la columna "score". De lo
    contrario devuelve el dataframe que no tenga el atributo o en ultima
    instancia un dataframe con valores nulos 

    Parameters
    ----------
    df : pandas.DataFrame
        pandas.DataFrame a concatenar

    df2 : pandas.DataFrame
        pandas.DataFrame a concatenar

    Returns
    -------
    pandas.DataFrame
        DataFrame concatenado
    """

    if "score" not in df.columns :
            if "score" in df2.columns:
                return df
            else:
                return pd.concat([df,df2])  
    else:
        dic = {}
        dic.update(
                creation_date_a  = [np.NaN],
                id = [np.NaN],#chekiar si es id or none
                score = [np.NaN]
                )
        return pd.DataFrame(dic)

def concat_df_question_or_nan(df,df2):

    """
    Concatena los dos dataframes si tienen la columna "score". De lo
    contrario devuelve el dataframe que tenga el atributo o en ultima
    instancia un dataframe con valores nulos 

    Parameters
    ----------
    df : pandas.DataFrame
        pandas.DataFrame a concatenar

    df2 : pandas.DataFrame
        pandas.DataFrame a concatenar

    Returns
    -------
    pandas.DataFrame
        DataFrame concatenado
    """

    if "score" in df.columns :
            if "score" not in df2.columns:
                return df
            else:
                return pd.concat([df,df2])  
    else:
        dic = {}
        dic.update(
                creation_date_q  = [np.NaN],
                id = [np.NaN]#chekiar si es id or none
                )
        return pd.DataFrame(dic)

def df_answer(chunk):
    """
    Toma una lista de Dataframes y los concatena. Finalmente
    devuelve un solo Dataframe sin valores nulos

    Parameters
    ----------
    chunk : list
        lista de pandas.Dataframes

    Returns
    -------
    df: pandas.Dataframe
        retorna un Dataframe
    """

    #devuelve lista de dfs Answer concatenados 
    lista_de_dfs = reduce(concat_df_answer_or_nan, chunk)
    lista_de_dfs = lista_de_dfs.dropna()

    return lista_de_dfs

def df_question(chunk):

    """
    Toma una lista de Dataframes y los concatena. Finalmente
    devuelve un solo Dataframe sin valores nulos

    Parameters
    ----------
    chunk : list
        lista de pandas.Dataframes

    Returns
    -------
    df: pandas.Dataframe
        retorna un Dataframe
    """

    #devuelve lista de dfs Questions concatenados 
    df = reduce(concat_df_question_or_nan, chunk)
    df = df.dropna()

    return df

# Reducers
#recibe lista de listas de datafremes
mapped = sys.stdin()

# reduce la lista de listas de dataframes a una lista de dataframes
reducida_1 = list(map(df_question , mapped)) 

# reduce la lista de dataframes a un solo dataframe
reducer_question_df = reduce(concat_df_question_or_nan,reducida_1)

# reduce la lista de listas de dataframes a una lista de dataframes
reducida_2 = list(map(df_answer , mapped))

# reduce la lista de dataframes a 1 solo dataframe
reducer_question_df = reduce(concat_df_answer_or_nan,reducida_2) 

#Merge 

# merge dataframes para tener solo answers que respondan questions con score 100~200
df_total = reducida_1.merge(reducida_2, how='inner', on='id')

# Se obtiene el el tiempo de respuesta de cada answer y se obtiene el promedio
fechas = (df_total['creation_date_a'] - df_total['creation_date_q']).dt.days
fecha_promedio = fechas.sum() / len(fechas)

# Tiempo promedio de respuesta
print(fecha_promedio)