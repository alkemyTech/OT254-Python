import pytest
import tests_elements 

import pandas as pd
from collections import Counter

# Task 1
from MapReduce_Hadoop.fechas_con_menos_posts_mapper import mapper,get_date
from MapReduce_Hadoop.fechas_con_menos_posts_reducer import reducer
# Task 2
from MapReduce_Hadoop.tiempo_respuesta_promedio_mapper import mapper as mapper_averege_time_response
from MapReduce_Hadoop.tiempo_respuesta_promedio_mapper import get_df
from MapReduce_Hadoop.tiempo_respuesta_promedio_reducer import df_answer, concat_df_answer_or_nan
from MapReduce_Hadoop.tiempo_respuesta_promedio_reducer import concat_df_question_or_nan, df_question
# Task 3
from MapReduce_Hadoop.top10_words_mapper import mapper as top10_mapper, get_words_of_body
from MapReduce_Hadoop.top10_words_reducer import reducer as top10_reducer, reducer_counters

#tests Task 1 mapper  
def test_get_date(root_row):
    """
    Check the function returns one string 
    with date
    """
    r = get_date(root_row)
    root = tests_elements.root()
    row = root[0]
    date = row.get("CreationDate")
    date = date.split('T')[0]
    r2 = date
    assert r == r2

def test_mapper(root_xml):
    """
    Check that the function takes a root 
    and returns a Counters of strings with the date
    """
    r = mapper(root_xml)
    r2 = tests_elements.counter_creationdate()
    assert r == r2

# Tests Task 1 reduce
def test_reduce(counter_word1: Counter, counter_word2: Counter):
    """
    Check the function reduce two Counters in
    a single counter
    """
    b = reducer(counter_word1, counter_word2)
    a = Counter("word1word2")
    assert a == b

# Tests Task mapper 2
def test_get_words_of_body(root_row):
    """
    check the fuction takes one xml.etree.ElementTree.Element
    get the item "Body" and return a Counter
    that counts each word in the Body string 
    """
    r = get_words_of_body(root_row)
    r2 = tests_elements.counter_words_body_row_1()
    assert r == r2


def test_top10_mapper(root_xml):
    """
    check that the function returns "counters" from an list of xml.elements
    and that it does not count special characters or numbers
    """
    r = top10_mapper(root_xml)
    r_impure = tests_elements.list_counters_impure_words_bodys()
    r_pure = tests_elements.list_counters_words_bodys()
    assert r != r_impure and r == r_pure

# Test Task 2  reduce
def test_top10_reducer(counter_word1: Counter, counter_word2: Counter):
    """
    Check the function reduce two Counters in
    a single counter
    """
    b = top10_reducer(counter_word1 ,counter_word2)
    a = Counter("word1word2")
    assert a == b

def test_reducer_counters(list_of_counters):
    """
    Check the function reduce a list with two Counters in
    a single counter
    """
    a = reducer_counters(list_of_counters)
    b = Counter("word1word2")       ######
    return a == b

# Test Task 3 reduces
def test_concat_df_question_or_nan(
                                    dataframe_with_creation_date_q,
                                    dataframe_with_creation_date_a
                                    ):
    """
    check the function concatenate two "dataframes" 
    and return a dataframe with three columns which one of them is score

    if one of them has not the colum Score or have less than
    three columns, the fuction return the other dataframe

    If bouth of this dataframe do not satisfy this requirements, 
    the function will return a Pandas.Dataframe
    with "id", "score", "creation_date_a" colums with numpy.Nan values
    """
    r = concat_df_question_or_nan(
                                    dataframe_with_creation_date_q,
                                    dataframe_with_creation_date_a
                                    )
    r2 = dataframe_with_creation_date_q
    assert r.equals(r2)

def test_concat_df_answer_or_nan(
                                dataframe_with_creation_date_q,
                                dataframe_with_creation_date_a
                                ):
    """
    check the function concatenate two "dataframes" 
    and return a dataframe with two columns and without column Score

    if one of them has the colum Score or have more than
    two columns, the fuction return the other dataframe

    If bouth of this dataframe do not satisfy this requirements, 
    the function will return a Pandas.Dataframe
    with "id", "creation_date_a" colums with numpy.Nan values

    """
    r = concat_df_answer_or_nan(
                                dataframe_with_creation_date_q,
                                dataframe_with_creation_date_a
                                )
    r2 = dataframe_with_creation_date_a
    assert r.equals(r2)


def test_df_answer(lista_dfs, dataframe_with_creation_date_a):
    """
    Chech the function returns one dataframe whit two columns
    from a list of two Dataframes
    """
    r = df_answer(lista_dfs)
    r2 = dataframe_with_creation_date_a
    assert r.equals(r2) 


def test_df_question(lista_dfs, dataframe_with_creation_date_q):
    """
    Chech the function returns one dataframe whit three columns
    from a list of two Dataframes
    """
    r = df_question(lista_dfs)
    r2 = dataframe_with_creation_date_q
    assert r.equals(r2)

# Test Task 3 mapper 
def test_get_df(root_row):
    """
    Check that the function from a ElementTree.elements, returns a dataframe
    made with the columns Id, Score, Create_date if Posttype is one,
    or that it returns a dataframe with the columns
    id, create_datetime columns
    """
    r = get_df(root_row)
    rr = get_df(root_row_pos(3))
    r2 = tests_elements.df_question()
    rr2 = tests_elements.df_answer()
    
    assert rr.equals(rr2) and r.equals(r2)  

def test_mapper_averege_time_response(list_two_rows_root):
    """
    Check the function returns a list of Pandas.Dataframes from 
    ElementTree.elements
    """
    r = mapper_averege_time_response(list_two_rows_root)
    r2 = tests_elements.lista_dfs()
    assert r[1].equals(r2[1]) and r[0].equals(r2[0])

# Test Fixtures

@pytest.fixture
def root_xml():
    root = tests_elements.root()
    return root

@pytest.fixture
def root_row():
    root = tests_elements.root()
    row = root[1] 
    return row

@pytest.fixture
def list_two_rows_root():
    root = tests_elements.root()
    row1 = root[1] 
    row2 = root[2] 
    return [row1,row2]

def root_row_pos(n):
    root = tests_elements.root()
    row = root[n]
    return row

@pytest.fixture
def root_row_createdate():
    root = tests_elements.root()
    row = root[0]
    date = row.get("CreationDate")
    date = date.split('T')[0]
    return date

@pytest.fixture
def counter_word1():
    counter = Counter("word1")
    return counter

@pytest.fixture
def counter_word2():
    counter = Counter("word2")
    return counter

@pytest.fixture
def list_of_counters():
    list_of_counters = [Counter("word1"),Counter("word2")]
    return list_of_counters

@pytest.fixture
def dataframe_with_creation_date_a():
    return  pd.DataFrame({
                        'creation_date_a' : [
                                            "7/12/2009",
                                            "18/8/2010",
                                            "29/4/2009"
                                            ],
                        "id":["id1","id2","id3"]
                        })

@pytest.fixture                      
def dataframe_with_creation_date_q():
    return  pd.DataFrame({
                        "creation_date_q" : [
                                            "2/12/2009",
                                            "9/8/2010",
                                            "4/4/2009"
                                            ],
                        'score' : [100,150,200],
                        "id" : ["id1","id2","id3"]
                        })

@pytest.fixture 
def lista_dfs(
            dataframe_with_creation_date_q,
            dataframe_with_creation_date_a,
            ):
    return [
            dataframe_with_creation_date_q,
            dataframe_with_creation_date_a,
            ]

@pytest.fixture 
def lista_dfs_with_nulls(
                        dataframe_with_creation_date_q,
                        dataframe_with_creation_date_a
                        ):
    return [
            dataframe_with_creation_date_q,
            dataframe_with_creation_date_a,
            ]
