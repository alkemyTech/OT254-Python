import unittest
import xml.etree.ElementTree as ET
import pandas as pd
from top_10_post import maper as maper1, reducer as reduce1, read_file as read_file1
from top_10_tiempo import maper as maper2, reducer_top_10_questions as reduce2, read_file as read_file2
from relation import maper as maper3, reducer_relation as reduce3, read_file as read_file3


class TestTop10Post(unittest.TestCase):
    def test_file(self):
        #Read the file
        self.assertIsInstance(read_file1('posts.xml'), ET.Element)    

    def test_mapper(self):    
        #Test maper
        self.assertEqual(len(maper1(read_file1('posts.xml'))),49569)

    def test_reducer(self):
        #Test reducer
        self.assertEqual(reduce1([[1,1],[2,1],[2,1],[3,1]]), [{'Count': 2, 'ViewCount': 2},{'Count': 1, 'ViewCount': 1}, {'Count': 1, 'ViewCount': 3}])

class TestTop10Time(unittest.TestCase):
    def test_file(self):
        #Read the file
        self.assertIsInstance(read_file2('posts.xml'), ET.Element)    

    def test_mapper(self):    
        #Test maper
        self.assertEqual(len(maper2(read_file2('posts.xml'))),41224)

    def test_reducer(self):
        #Test reducer
        result = [[1,203],[3,90],[2,24],[4,18]]
        df = pd.DataFrame(result)
        df.columns = ['Id', 'Time']
        df = df.values.tolist()
        self.assertEqual(reduce2([[1,203],[2,24],[3,90],[4,18]]).values.tolist(), df)
    
class TestRelation(unittest.TestCase):
    
    def test_file(self):
        #Read the file
        self.assertIsInstance(read_file3('posts.xml'), ET.Element)    

    def test_mapper(self):    
        #Test maper
        self.assertEqual(len(maper3(read_file3('posts.xml'))),13946)

    def test_reducer(self):
        #Test reducer
        self.assertEqual(reduce3([(.2, .3), (.0, .6), (.6, .0), (.2, .1)]).values.tolist(), [[1.0, 0.3], [0.3, 1.0]])

if __name__ == '__main__':
    unittest.main()