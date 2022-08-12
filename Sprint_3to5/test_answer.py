import unittest
from xmlparser import get_data, chunkify
from answer_v2 import get_col_p, get_col_c, mapper_p, mapper_c, reducer
from pathlib import Path
from pandas import Timedelta

TEST_DATA_DIR_P = str(Path(__file__).resolve().parent / 'tests_data' / 'Posts_test.xml')
TEST_DATA_DIR_C = str(Path(__file__).resolve().parent / 'tests_data' / 'Comments_test.xml')
LENGTH = 10
RAW_LIST_P = [['4', '2008-07-31T21:42:52.667'], 
                   ['6', '2008-07-31T22:08:08.620'], 
                   ['7', '2008-07-31T22:17:57.883'], 
                   ['8', '2008-07-31T23:33:19.290'], 
                   ['9', '2008-07-31T23:40:59.743'], 
                   ['11', '2008-07-31T23:55:37.967'], 
                   ['12', '2008-07-31T23:56:41.303'], 
                   ['13', '2008-08-01T00:42:38.903'], 
                   ['14', '2008-08-01T00:59:11.177'], 
                   ['16', '2008-08-01T04:59:33.643'], 
                   ['17', '2008-08-01T05:09:55.993'], 
                   ['18', '2008-08-01T05:12:44.193'], 
                   ['19', '2008-08-01T05:21:22.257'], 
                   ['20', '2008-08-01T07:25:22.443'], 
                   ['21', '2008-08-01T08:57:27.280'], 
                   ['22', '2008-08-01T12:07:19.500'], 
                   ['23', '2008-08-01T12:09:41.947'], 
                   ['24', '2008-08-01T12:12:19.350'], 
                   ['25', '2008-08-01T12:13:50.207'], 
                   ['26', '2008-08-01T12:16:22.167'], 
                   ['27', '2008-08-01T12:17:19.357'], 
                   ['28', '2008-08-01T12:17:41.463'], 
                   ['29', '2008-08-01T12:19:17.417'], 
                   ['30', '2008-08-01T12:22:40.797'], 
                   ['31', '2008-08-01T12:22:51.593'], 
                   ['32', '2008-08-01T12:23:39.967'], 
                   ['33', '2008-08-01T12:26:39.773'], 
                   ['34', '2008-08-01T12:30:57.630'], 
                   ['35', '2008-08-01T12:32:48.700'], 
                   ['36', '2008-08-01T12:35:56.917'], 
                   ['37', '2008-08-01T12:36:00.957'], 
                   ['39', '2008-08-01T12:43:11.503'], 
                   ['41', '2008-08-01T12:49:12.413'], 
                   ['42', '2008-08-01T12:50:18.587'], 
                   ['43', '2008-08-01T12:53:19.173'], 
                   ['44', '2008-08-01T12:55:42.413'], 
                   ['45', '2008-08-01T12:56:37.920'], 
                   ['48', '2008-08-01T13:01:17.303'], 
                   ['49', '2008-08-01T13:02:51.900'], 
                   ['51', '2008-08-01T13:07:52.810'], 
                   ['52', '2008-08-01T13:08:59.127'], 
                   ['53', '2008-08-01T13:08:59.487'], 
                   ['54', '2008-08-01T13:09:18.970'], 
                   ['55', '2008-08-01T13:10:03.930'], 
                   ['56', '2008-08-01T13:10:16.473'], 
                   ['58', '2008-08-01T13:14:30.303'], 
                   ['59', '2008-08-01T13:14:33.797'], 
                   ['60', '2008-08-01T13:14:40.007'], 
                   ['61', '2008-08-01T13:17:20.640']]
RAW_LIST_C = [['4', '2008-09-06T08:07:10.730'], 
                   ['5', '2008-09-06T08:42:16.980'], 
                   ['6', '2008-09-06T10:30:26.330'], 
                   ['7', '2008-09-06T11:51:50.207'], 
                   ['4', '2008-09-06T12:26:30.060'], 
                   ['6', '2008-09-06T13:38:23.647'], 
                   ['1', '2008-09-06T13:51:47.843'], 
                   ['2', '2008-09-06T14:15:46.897'], 
                   ['60', '2008-09-06T14:30:40.217'], 
                   ['40', '2008-09-06T14:42:35.303'], 
                   ['10', '2008-09-06T15:02:40.980'], 
                   ['6', '2008-09-06T15:11:11.227'], 
                   ['17', '2008-09-06T15:44:39.477'], 
                   ['8', '2008-09-06T16:00:35.147'], 
                   ['26', '2008-09-06T16:46:31.450'], 
                   ['38', '2008-09-06T17:33:30.417'], 
                   ['26', '2008-09-06T17:37:35.757'], 
                   ['49', '2008-09-06T19:15:41.277'], 
                   ['49', '2008-09-06T19:25:04.703'], 
                   ['49', '2008-09-06T19:40:51.123'], 
                   ['50', '2008-09-06T20:14:30.777'], 
                   ['77', '2008-09-06T20:21:55.457'], 
                   ['13', '2008-09-06T20:33:10.610'], 
                   ['45', '2008-09-06T20:44:00.927'], 
                   ['45', '2008-09-06T21:40:25.923'], 
                   ['45', '2008-09-06T22:13:15.563'], 
                   ['45', '2008-09-06T22:32:45.003'], 
                   ['47', '2008-09-06T22:42:13.623'], 
                   ['48', '2008-09-06T23:00:58.880'], 
                   ['47', '2008-09-06T23:11:04.613'], 
                   ['49', '2008-09-06T23:15:49.860'], 
                   ['49', '2008-09-06T23:18:15.580'], 
                   ['47', '2008-09-06T23:20:45.353'], 
                   ['47', '2008-09-06T23:27:15.510'], 
                   ['45', '2008-09-07T00:13:43.713'], 
                   ['38', '2008-09-07T00:14:10.983'], 
                   ['49', '2008-09-07T00:18:11.643'], 
                   ['25', '2008-09-07T00:25:07.773'], 
                   ['46', '2008-09-07T00:37:17.870'], 
                   ['44', '2008-09-07T00:49:12.520'], 
                   ['40', '2008-09-07T01:09:55.887'], 
                   ['44', '2008-09-07T01:55:26.230'], 
                   ['24', '2008-09-07T02:17:23.370'], 
                   ['21', '2008-09-07T02:38:14.537'], 
                   ['58', '2008-09-07T02:43:57.440'], 
                   ['60', '2008-09-07T02:50:27.970'], 
                   ['74', '2008-09-07T02:51:37.203'], 
                   ['48', '2008-09-07T03:00:30.333'], 
                   ['48', '2008-09-07T03:09:36.163'], 
                   ['40', '2008-09-07T03:23:43.930'],
                   ['41', '2008-09-07T03:31:19.607'], 
                   ['43', '2008-09-07T03:39:33.937'], 
                   ['41', '2008-09-07T03:44:06.703'], 
                   ['45', '2008-09-07T04:42:56.673'], 
                   ['48', '2008-09-07T06:47:04.127'], 
                   ['14', '2008-09-07T08:37:54.657'], 
                   ['12', '2008-09-07T08:40:16.337']]
DELAY = 36

class test_answer(unittest.TestCase):
    """
    This tests the funtions to obtain the Average Time between the Post Date and the Date of its first Comment. Using two test input files 
    it checks each function individually.
    """
    def setUp(self):
        self.data_p = chunkify(get_data(TEST_DATA_DIR_P), LENGTH)
        self.data_c = chunkify(get_data(TEST_DATA_DIR_C), LENGTH)
        
        self.data2_p = list(map(mapper_p, self.data_p))
        self.data2_c = list(map(mapper_c, self.data_c))

    def test_get_col_p_raw(self):
        "Checks that all posts in the test file are being parsed"
        for chunk in self.data_p:
            for row in chunk:
                with self.subTest(row):
                    self.assertIn(get_col_p(row), RAW_LIST_P)
    
    def test_get_col_c_raw(self):
        "Checks that all comments in the test file are being parsed"
        for chunk in self.data_c:
            for row in chunk:
                with self.subTest(row):
                    self.assertIn(get_col_c(row), RAW_LIST_C)
                    
    def test_mapper_p_type(self):
        "Checks that the correct instance is created"
        for chunk in self.data_p:
            with self.subTest(chunk):
                self.assertIsInstance(mapper_p(chunk), list)
    
    def test_mapper_c_type(self):
        "Checks that the correct instance is created"
        for chunk in self.data_c:
            with self.subTest(chunk):
                self.assertIsInstance(mapper_c(chunk), list)
            
    def test_reducer_type(self):
        "Checks that the correct instance is created"
        df, reduced = reducer(self.data2_p, self.data2_c)
        self.assertIsInstance(reduced, Timedelta)
    
    def test_reducer_df_integrity(self):
        "Checks that there are not None values in the dataframe (It should not have Posts with no Comments)"
        df, reduced = reducer(self.data2_p, self.data2_c)
        dic = list(df.all().notnull().to_dict().values())
        for v in dic:
            with self.subTest(v):
                self.assertTrue(v)
    
    def test_reducer_result(self):
        "Checks that the final result is the one of the testing files (in days)"
        df, reduced = reducer(self.data2_p, self.data2_c)
        self.assertEqual(reduced.days, DELAY)

