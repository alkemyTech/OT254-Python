"""
Este modulo es un mapreducer para extraer el puntaje promedio de las repuestas con mas favoritos
de el dataset de stackoverflow
"""

import xml.etree.ElementTree as ET
import pandas as pd 


def mapper():
    """
    funcion para extrar de el dataset una lista que contenga el puntaje promedio de las repuestas con mas favoritos

    return: lista que contiene el puntaje de las repuestas con mas favoritos
    """
    #se abre el archivo que contiene los datos
    tree = ET.parse('outp.xml')
    lista = []
    #se itera sobre cada elemento row de el dataset
    for node in tree.iter('row'):
        #se extrae el numero de favoritos
        favoritecount = node.attrib.get('FavoriteCount')
        #se extrae el puntaje 
        score = node.attrib.get('Score')
        #se comprueba que FavoriteCount contenga un valor
        if favoritecount != None:
            #si se cumplen las condiciones se agrega el tag a la lista
            lista.append(int(score))
    return lista




def reducer(data):
    """
    funcion para calcular el puntaje promedio

    data : recibe una lista con los puntajes

    return : retorna el puntaje pormedio 

    """
    #se crea un dataframe con los puntajes
    df = pd.DataFrame(data)
    #se le asigna un nombre a la columna de el dataframe
    df.columns = ['score']
    #se calcula el promedio
    promedio = df['score'].mean()
    return promedio



if __name__ == "__main__":
    data = mapper() 
    print (reducer(data))
