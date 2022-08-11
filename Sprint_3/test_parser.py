import unittest
from xmlparser import get_data, chunkify
from pathlib import Path
import xml.etree.ElementTree as ET

TEST_DATA_DIR = str(Path(__file__).resolve().parent / 'tests_data' / 'Comments_test.xml')
TEST_DATA_DIR_EX = str(Path(__file__).resolve().parent / 'tests_data' / 'Posts_test.txt')
LENGTH = 10


class test_xmlparser(unittest.TestCase):
    
    def setUp(self):
        self.data = get_data(TEST_DATA_DIR)
        
    def test_data_processing_type(self):
        "Checks that the correct instance is created"
        self.assertIsInstance(self.data, ET.Element)
    
    def test_data_processing_type_exeption(self):
        "Checks that the exception is raised if the files is not of the right extension"
        with self.assertRaises(Exception) as context:
            get_data(TEST_DATA_DIR_EX)
    
    def test_files_root_tag(self):
        "Checks that the file root tag is coorrect"
        self.assertIn(self.data.tag, ["posts", "comments"])
        
    def test_chunks_type(self):
        "Checks that the correct instance is created"
        self.assertIsInstance(chunkify(self.data, LENGTH), list)
    
    def test_chunks_length(self):
        "Checks that the data spliting gets done rigth"
        for chunk in chunkify(self.data, LENGTH):
            with self.subTest(chunk):
               self.assertLessEqual(len(chunk), LENGTH)