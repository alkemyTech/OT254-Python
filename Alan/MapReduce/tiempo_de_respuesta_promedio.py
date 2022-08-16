
"""
toma los post de tipo Question con score de entre 100~200 y tambien toma las
respuestas devolviendo el tiempo promedio de respuesta 

"""
from pathlib import Path
import xml.etree.ElementTree as ET
from functools import reduce
import pandas as pd

def get_df(row):

    """
    Obtiene el atributo "PostTypeId" y dependiendo del valor devuelve Dataframe
    con ["Id","Score","CreationDate"] si el PostTypeId es 1(Question) o devuelve
    un Dataframe con ["CreationDate","ParentID"] si PostTypeId es 2(Answer) 

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    pandas.Dataframe
        returns dataframe
    """
    dic = {}

    parent_or_answer = row.get("PostTypeId")
    date = row.get("CreationDate")
    date = date.split('T')[0]

    # Si el Score no esta entre 100~200 devuelve un Dataframe con valores nulos
    if parent_or_answer == 1:
        score = row.get("Score")
        if score >200 or score <100:
            dic.update(
                id = [NaN],
                score = [NaN],
                creation_date_q  = [NaN]            ##### to datetime dale
                )
        else:
            dic.update(
                    id = [row.get("Id")],
                    score = [row.get("Score")],
                    creation_date_q  = [date]        ##### to datetime dale
                    )

    elif parent_or_answer == 2:
        dic.update(
                creation_date_a  = [date],         ##### to datetime dale
                id = [row.get("ParentID")]#chekiar si es id or none
                )

    return pd.DataFrame(dic)

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
                creation_date_a  = [Nan],
                id = [Nan],#chekiar si es id or none
                score = [NaN]
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
                creation_date_q  = [Nan],
                id = [Nan]#chekiar si es id or none
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

def mapper(chunk):
    """
    A cada row de chunk le aplica la funcion get_df y devuelve una lista
    de DataFrames 

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    list
        retorna una lista de Dataframes
    """
    lista_de_dfs = list(map(get_df, chunk))
    
    return lista_de_dfs


if __name__ == '__main__':

    # Path file
    path_file = Path.joinpath(Path.cwd().parent.parent, "Desktop","112010 Stack Overflow","posts.xml" )

    try:
        # Parses the .xml file
        tree = ET.parse(path_file)
        data = tree.getroot()

    except FileNotFoundError as e:
        print("err")
    else:
        
        # Chunkify data
        chunk_len = 50
        chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]

        # Mapper que devuelve lista de listas de dfs
        mapped = list(map(mapper, chunks))

        # Reducers

        #reduce los dataframes de las listas de listas a uno solo dataframe por lista
        reducida_1 = list(map(df_question , mapped)) 

        # reduce la lista de dataframes a 1 solo dataframe
        reducer_question_df = reduce(concat_df_question_or_nan,reducida_1)
        
        # reduce los dataframes de las listas de listas a uno solo dataframe por lista
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
        print(f"El tiempo de respuesta promedio a posts con score entre 100~200 es de : {fecha_promedio}")