import unittest
from xmlparser import get_data, chunkify
from words_v2 import words_score, mapper, reducer
from pathlib import Path

TEST_DATA_DIR = str(Path(__file__).resolve().parent / 'tests_data' / 'Posts_test.xml')
LENGTH = 10
LIST = [[77, 42],
            [93, 13],
            [37, 59],
            [12, 7],
            [13, 122],
            [25, 149],
            [196, 48],
            [31, 69],
            [10, 45],
            [145, 10],
            [10, 15],
            [180, 6],
            [434, 60],
            [22, 3],
            [363, 1],
            [29, 2],
            [103, 11],
            [23, 4],
            [61, 9],
            [49, 18],
            [167, 3],
            [31, -3],
            [51, 3],
            [55, 0],
            [34, 9],
            [11, 0],
            [65, 111],
            [36, 5],
            [41, 2],
            [83, 13],
            [96, 0],
            [136, 3],
            [39, 4],
            [68, 40],
            [72, 6],
            [51, 2],
            [68, 4],
            [140, 14],
            [30, 3],
            [50, 1],
            [72, 1],
            [143, 2],
            [34, 0],
            [70, 2],
            [87, 6],
            [63, 13],
            [48, 4],
            [86, 0],
            [42, 2]]

class test_answer(unittest.TestCase):
    """
    This tests the funtions to obtain the correlation between the lenght of the Body (in words) of each Post with its Score. Using a test input file 
    it checks each function individually.
    """
    def setUp(self):
        self.data = chunkify(get_data(TEST_DATA_DIR), LENGTH)
        self.data2 = list(map(mapper, self.data))

    def test_words_score_return_type(self):
        "Checks that the correct instance is created"
        for chunk in self.data:
            for row in chunk:
                with self.subTest(row):
                    self.assertIsInstance(words_score(row), list)
    
    def test_words_score_body_count(self):
        "Checks that the body lenght of each post is greter than cero"
        for chunk in self.data:
            for row in chunk:
                with self.subTest(row):
                    self.assertGreater(words_score(row)[0], 0)
                    
    def test_mapper_type(self):
        "Checks that the correct instance is created"
        for chunk in self.data:
            with self.subTest(chunk):
                self.assertIsInstance(mapper(chunk), list)
    
    def test_mapper(self):
        "Checks that the result of each mapping is in the final 'pair' list (['body length', 'score'])"
        for chunk in self.data:
            for body in range(0,len(chunk)-1):
                with self.subTest(body):
                    self.assertIn(mapper(chunk)[body], LIST)
            
    def test_reducer(self):
        "Checks that the result is between -1 and 1"
        reduced = reducer(self.data2)
        self.assertGreaterEqual(reduced, -1)
        self.assertLessEqual(reduced, 1)

