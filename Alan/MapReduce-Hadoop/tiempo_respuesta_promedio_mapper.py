
"""
toma los post de tipo Question con score de entre 100~200 y tambien toma las
respuestas devolviendo el tiempo promedio de respuesta 

"""
from pathlib import Path
import xml.etree.ElementTree as ET
from functools import reduce
import fileinput
import numpy as np
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

    parent_or_answer = int(row.get("PostTypeId"))
    date = row.get("CreationDate")
    date = date.split('T')[0]

    # Si el Score no esta entre 100~200 devuelve un Dataframe con valores nulos
    if parent_or_answer == 1:
        score = int(row.get("Score"))
        if score >200 or score <100:
            dic.update(
                id = [np.NaN],
                score = [np.NaN],
                creation_date_q  = [np.NaN]            
                )
        else:
            dic.update(
                    id = [int(row.get("Id"))],
                    score = [int(row.get("Score"))],
                    creation_date_q  = [date]        
                    )

    elif parent_or_answer == 2:
        dic.update(
                creation_date_a  = [date],         
                id = [int(row.get("ParentID"))]
                )

    return pd.DataFrame(dic)

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

if __name__ == "__main__":
    # Parsea el .xml file que recive en fileinput.input()
    tree = ET.parse(fileinput.input())
    data = tree.getroot()

    # Chunkify data
    chunk_len = 50
    chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]

    # Mapper que devuelve lista de listas de dfs
    mapped = list(map(mapper, chunks))

    print(mapped)