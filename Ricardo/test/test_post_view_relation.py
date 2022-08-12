'''
modulo para realizar prubeas unitarias del mapreducer 
que extrae la relación entre cantidad de palabras en un post y su cantidad de visitas
de el dataset de stackoverflow

para ejecutarlo se utiliza el comando : 
    pytest
'''


from mapreduce.post_view_relation import chunkify,mapper,reducer
import xml.etree.ElementTree as ET


#se define el dataset sobre el cual se va a realizar el test
file = 'post.xml'

#se define una variavle con los datos para testear la funcion mapper
data_tets_mapper = ET.parse(file).getroot()

#se define una variavle con los datos para testear la funcion reducer
data_test_reducer = [
    [1414, 0], 
    [183, 0], 
    [305, 0], 
    [799, 0], 
    [705, 906], 
    [974, 0], 
    [420, 0], 
    [144, 0], 
    [401, 0], 
    [1277, 0], 
    [482, 1501], 
    [749, 0], 
    [242, 0], 
    [231, 0], 
    [312, 0]
]

def test_chunkify():
    assert chunkify(file,10) != []

def test_mapper():
    assert mapper(data_tets_mapper) != []

def test_reducer():
    assert reducer(data_test_reducer) != []