import unittest
from xmlparser import get_data, chunkify
from top10_v2 import get_tags, mapper, reducer
from functools import reduce
from pathlib import Path
from collections import Counter

TEST_DATA_DIR = str(Path(__file__).resolve().parent / 'tests_data' / 'Posts_test.xml')
LENGTH = 10
RAW_TAGS_LIST = [None, 
             ['c#'],
             ['html', 'css', 'internet-explorer-7'],
             ['c#', 'conversion', 'j#'],
             ['c#', 'datetime'],
             ['c#', '.net', 'datetime', 'timespan'],
             ['html', 'browser', 'time', 'timezone'],
             ['c#', 'linq', 'web-services', '.net-3.5'],
             ['mysql', 'database'],
             ['php'],
             ['mysql', 'database', 'triggers'],
             ['c++', 'c', 'sockets', 'mainframe', 'zos'],
             ['sql-server', 'datatable'],
             ['c#', '.net', 'vb.net', 'timer'],
             ['php', 'architecture', 'plugins'],
             ['html', 'form-submit'],
             ['c#', 'linq', '.net-3.5'],
             ['office-2007', 'filetypes']]
TAG_LIST = [None, 
            'c#', 'html', 'datetime', '.net', 'linq', '.net-3.5', 'mysql', 'database', 'php', 'css', 'internet-explorer-7', 'conversion', 'j#', 'timespan', 'browser', 'time', 'timezone', 'web-services', 'triggers', 'c++', 'c', 'sockets', 'mainframe', 'zos', 'sql-server', 'datatable', 'vb.net', 'timer', 'architecture', 'plugins', 'form-submit', 'office-2007', 'filetypes']

class test_top10(unittest.TestCase):
    """
    This tests the funtions to obtain the Top 10 Tags used in Posts with Accepted Answers. Using a test input file 
    it checks each function individually.
    """
    def setUp(self):
        self.data = chunkify(get_data(TEST_DATA_DIR), LENGTH)
        self.data2 = list(map(mapper, self.data))

    def test_get_tags_raw(self):
        "Checks that all tags of the test file are being parsed"
        for chunk in self.data:
            for row in chunk:
                with self.subTest(row):
                    self.assertIn(get_tags(row), RAW_TAGS_LIST)

    def test_mapper_type(self):
        "Checks that the correct instance is created"
        for chunk in self.data:
            with self.subTest(chunk):
                self.assertIsInstance(mapper(chunk), Counter)
    
    def test_mapper_tags(self):
        "Checks that the cleaned tag list is complete"
        for chunk in self.data:
            for tag in list(mapper(chunk).keys()):
                with self.subTest(tag):
                    self.assertIn(tag, TAG_LIST)
            
    def test_mapper_tags_sample(self):
        "Checks that tags are being counted correctly (with a sample)"
        reduced = reduce(reducer, self.data2)
        self.assertEqual(reduced['c#'], 7)
        self.assertEqual(reduced['.net'], 2)
        self.assertEqual(reduced['c++'], 1)
    
    def test_mapper_tags_order(self):
        "Checks that the final result is ordered in descending order"
        reduced = reduce(reducer, self.data2).most_common(10)
        for i in range(0, len(reduced)-1):
            with self.subTest(i):
                self.assertLessEqual(reduced[i+1][1], reduced[i][1])
