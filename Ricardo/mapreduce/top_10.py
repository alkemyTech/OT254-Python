"""
Este modulo es un mapreducer para extraer el top 10 de los tags sin respuesta aceptada 
de el dataset de stackoverflow
"""

from ast import If
import xml.etree.ElementTree as ET
import operator



def chunkify(file, chunk_len): 
    tree = ET.parse(file)
    data = tree.getroot()
    chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]
    return chunks


def mapper(data):
    """
    funcion para extraer los tags sin respuesta aceptadas del dataset

    return : retorna una lista que contiene los tags que no tienen respuestas
    aceptadas
    """
    #se lee el archivo xml que contiene los datos
    tag_list = []
    #se itera sobre los elementos row en el archivo xml
    for node in data.iter('row'):
        #se obtiene el valor tag por cada row
        tags = node.attrib.get('Tags')
        #se obtiene el valor de AcceptedAnswerId por cada row
        AcceptedAnswer = node.attrib.get('AcceptedAnswerId')
        #se comprubea si es un tag valido y si no tiene respuestas aceptadas
        if AcceptedAnswer == None and tags != None:
            #si se cumplen las condiciones se agrega el tag a la lista
            tag_list.append(tags)
    return tag_list


def reducer(data):
    """
    funcion para reducir el top 10 de los tag sin respuesta aceptada

    data : recibe una lista con los tags

    return : retrona una lista que contiene el top 10
    de los tags sin respuesta aceptada

    """
    reducer = {}
    #se itera por cada tag que hay en la lista
    for n in data:
        #se comprueba si el tag ya existe en el diccionario de ser asi se incrementa en 1
        #de lo contrario se establece como primer valor 1 
        if n in reducer:
            reducer[n] += 1
        else:
            reducer[n] = 1 
    #se cuenta la cantidad total de cada tag, se ordena de mayor a menor y se guarda en una lista 
    reducer = sorted(reducer.items(), key=operator.itemgetter(1), reverse=True)
    #se extrae los 10 primeros valores de la lista
    reducer = reducer[:10]
    return reducer


if __name__ == "__main__":
    try:
        data = chunkify('post.xml')
    except FileNotFoundError as e:
        print('error al abrir el archivo')
    else:
        datamaped = mapper(data)
        print(reducer(datamaped))