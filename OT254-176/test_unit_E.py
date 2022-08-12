from typing import List
import unittest
from mapReduce import reducer,  mapper as  tag_mapper, calculate_top_10, tags_accepted_answers, relation_reducer, relation_mapped
from pathlib import Path
import xml.etree.ElementTree as ET
import logging
import mock

logging.config.fileConfig('./config/logging.cfg')
file_logger = logging.getLogger('fileRotating')
console_logger = logging.getLogger('console')


xml_path1 = './Stack Overflow 11-2010/112010 Meta Stack Overflow/posts.xml'
csv_name1 = './xlmtocsv.csv'

class test_map_reduce_task_1(unittest.TestCase):
    #Clase de prueba para la tarea:
    #> Top 10 fechas con mayor cantidad de post
     
     def test_tag_mapper_return_type(self):
        #Comprobando el tipo de devolución de tag_mapper
        self.assertIsInstance(tag_mapper(root), dict)

     def test_tag_mapper_wrong_data(self):
        #Usar un archivo con diferente estructura xml
        #para comprobar si la función devuelve un diccionario vacío
        pathXml_2 = 'users.xml'
        tree_2 = ET.parse(pathXml_2)
        root_2 = tree_2.getroot()
        self.assertEqual(tag_mapper(root_2), {})
     
     def test_tag_mapper_count(self):
        # carga de archivos xml y data
        pathXml_3 = 'posts_test.xml'
        tree_3 = ET.parse(pathXml_3)
        root_3 = tree_3.getroot()
        # definición de los tipos de etiquetas esperados y su conteo que debe
        # devolver la función probada
        dict_out = {'discussion': 4,
                    'status-completed': 4,
                    'uservoice': 4
                    }

        self.assertEqual(tag_mapper(root_3), dict_out)

     def test_tags_acceptedAnswers_attrib_err(self):
        #Verifique la Excepción que se genera cuando un objeto
        #tiene un nombre de atributo incorrecto.
        obj = mock.Mock()
        obj.PostTypeId = "1"
        obj.Accepted_answerWrongAttrib = "2"

        with self.assertRaises(Exception):
            tags_accepted_answers(obj)
     
     def test_calculate_top_10(self):
        #Para un diccionario con valores enteros,
        #verifique los 10 mejores artículos según sus valores.
        # diccionario de entrada
        mock_dict = {'category_1': 120, 'category_2': 119,
                     'category_3': 119, 'category_4': 100,
                     'category_5': 90,  'category_6': 80,
                     'category_7': 70,  'category_8': 60,
                     'category_9': 50,  'category_10': 45,
                     'category_11': 40
                     }
        # diccionario de salida
        mock_dict_out = {'category_1': 120, 'category_2': 119,
                         'category_3': 119, 'category_4': 100,
                         'category_5': 90,  'category_6': 80,
                         'category_7': 70,  'category_8': 60,
                         'category_9': 50,  'category_10': 45,
                         }

        self.assertEqual(calculate_top_10(mock_dict), mock_dict_out)

class test_map_reduce_task_2(unittest.TestCase):
    #Clase de prueba para la tarea:
        #> Relación entre cantidad de respuestas en un post y su cantidad de visitas
    
    def test_relation_mapped_expected_output(self):
        pathXml = 'posts_test.xml'

        expected_count = [{2: 13},
                          {3: 2},
                          {2: 50},
                          {1: 10}
                         ]
        self.assertEqual(relation_mapped(pathXml), expected_count)

    def test_relation_reducer_empty_list(self):
        #'Función relación_reductor probada con un parámetro vacío, se espera que devuelva un diccionario vacío
        empty_list = []

        self.assertEqual(relation_reducer(empty_list), {})


if __name__ == "__main__":
    unittest.main()

    
     