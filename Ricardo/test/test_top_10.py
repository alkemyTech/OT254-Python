'''
modulo para realizar prubeas unitarias del mapreducer 
que extrae el top 10 de los tags sin respuesta aceptada 
de el dataset de stackoverflow

para ejecutarlo se utiliza el comando : 
    pytest
'''

from mapreduce.top_10 import chunkify,mapper,reducer
import xml.etree.ElementTree as ET



#se define el dataset sobre el cual se va a realizar el test
file = 'post.xml'

#se define una variavle con los datos para testear la funcion mapper
data_tets_mapper = ET.parse(file).getroot()

#se define una variavle con los datos para testear la funcion reducer
data_test_reducer = [
    '<java><tomcat><centos>', 
    '<java><xml><soap><namespaces><axis>', 
    '<api><rest><game-development><engine><middleware>', 
    '<ajax><image><caching><internet-explorer-6>', 
    '<xml><ksh>', 
    '<language-agnostic><api><dictionary><natural-language>', 
    '<sql><mysql><schema>', '<xml><tools><diff>', 
    '<c#><.net-3.5><compiler><.net-2.0>', '<c++><qt>', 
    '<direct3d><alphablending><compositing>', 
    '<windows><gdi+>', '<open-source>', 
    '<java><spring><webflow>', 
    '<debugging><iis><crash><troubleshooting>', 
    '<windows><graphics><gdi+>', 
    '<linux><operating-system><mutex><semaphore><glossary>']


def test_chunkify():
    assert chunkify(file,10) != []

def test_mapper():
    assert mapper(data_tets_mapper) != []

def test_reducer():
    assert reducer(data_test_reducer) != []