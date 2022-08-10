'''
modulo para realizar prubeas unitarias del mapreducer 
que extrae el puntaje promedio de las repuestas con mas favoritos
de el dataset de stackoverflow

para ejecutarlo se utiliza el comando : 
    pytest
'''

from mapreduce.average import chunkify,mapper,reducer
import xml.etree.ElementTree as ET

#se define el dataset sobre el cual se va a realizar el test
file = 'post.xml'

#se define una variavle con los datos para testear la funcion mapper
data_tets_mapper = ET.parse(file).getroot()

#se define una variavle con los datos para testear la funcion reducer
data_test_reducer =[
    8, 21, 10, 8, 0, 14, 3, 6, 33, 4, 6, 8, 
    3, 7, 2, 12, 43, 7, 4, 11, 8, 4, 22, 14,
    22, 2, 25, 0, 6, 5, 14, 9, 4, 12, 2, 10, 
    10, 4, 3, 17, 14, 31, 16, 1, 3, 9, 10, 30, 
    4, 3, 1, 6, 13, 3, 310, 16, 16, 17, 6, 26, 
    1, 13, 9, 15, 1, 13, 5, 35, 7, 4, 6, 4, 1,
    14, 88, 22, 3, 7, 2, 1, 2, 2, 11, 8, 5, 70
]



def test_chunkify():
    assert chunkify(file,10) != []

def test_mapper():
    assert mapper(data_tets_mapper) != []

def test_reducer():
    assert reducer(data_test_reducer) != []