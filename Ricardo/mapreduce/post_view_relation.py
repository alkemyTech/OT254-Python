"""
Este modulo es un mapreducer para extraer la relación entre cantidad de palabras en un post y su cantidad de visitas
de el dataset de stackoverflow

"""

import xml.etree.ElementTree as ET
import pandas as pd 


def chunkify(file, chunk_len): 
    tree = ET.parse(file)
    data = tree.getroot()
    chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]
    return chunks

def mapper(data):
    """
    funcion para extrar de el dataset una lista que contenga las palabras por post y su cantidad de visitas

    return: lista que contiene las palabras por post y la cantidad de visitas 
    """
    #se abre el archivo que contiene los datos
    lista = []
    #se itera sobre los elementos row de el dataset
    for node in data.iter('row'):
        #se extrae la cantidad de palabras por cada post
        word_by_post = len(node.attrib.get('Body'))
        #se extrae la cantidad de visitas por post
        ViewCount = node.attrib.get('ViewCount')
        #se agregan los datos a la lista
        lista.append([word_by_post, int(ViewCount)])
    return lista



def reducer(data):
    """
    funcion para obtener la relacion entre la cantidad de palabras que contiene un post y sus visitas

    data  : recibe como parametro una lista que contiene la cantidad de palabras y visitas que corresponde a cada post

    return : relacion entre palabras por posts y visitas
    """
    #se crea un dataframe con la lista recibida como parametro 
    df = pd.DataFrame(data)
    #se le da nombre a las columnas del dataframe
    df.columns = ['word_by_post', 'ViewCount']
    #se calcula la correlacion entre las palabras por post y sus visitas
    relacion = df.corr()
    return relacion['word_by_post'][1]

if __name__ == "__main__":
    try:
        data = chunkify('post.xml')
    except FileNotFoundError as e:
        print('error al abrir el archivo')
    else:
        datamaped = mapper(data)
        print(reducer(datamaped))