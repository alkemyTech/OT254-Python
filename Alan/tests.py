

from pathlib import  Path
import sys
p=sys.path.append(Path.joinpath(Path(__file__).parent.parent))

import pytest
import tests_elements 

import pandas as pd
from collections import Counter

# Tarea 1
from MapReduce_Hadoop.fechas_con_menos_posts_mapper import mapper,get_date
from MapReduce_Hadoop.fechas_con_menos_posts_reducer import reducer
# Tarea 2
from MapReduce_Hadoop.tiempo_respuesta_promedio_mapper import mapper as mapper_averege_time_response
from MapReduce_Hadoop.tiempo_respuesta_promedio_mapper import get_df
from MapReduce_Hadoop.tiempo_respuesta_promedio_reducer import df_answer, concat_df_answer_or_nan
from MapReduce_Hadoop.tiempo_respuesta_promedio_reducer import concat_df_question_or_nan, df_question
# Tarea 3
from MapReduce_Hadoop.top10_words_mapper import mapper as top10_mapper, get_words_of_body
from MapReduce_Hadoop.top10_words_reducer import reducer as top10_reducer, reducer_counters

#tests tarea 1 mapper  
def test_get_date(root_row):
    r = get_date(root_row)
    root = tests_elements.root()
    row = root[0]
    date = row.get("CreationDate")
    date = date.split('T')[0]
    r2 = date
    assert r == r2

def test_mapper(root_xml):
    r = mapper(root_xml)
    r2 = tests_elements.counter_creationdate()
    assert r == r2

# Tests tarea 1 reduce
def test_reduce(counter1: Counter, counter2: Counter):
    b = reducer(counter1,counter2)
    a = Counter("holahola12")
    assert a == b

# Tests tarea maper 2
def test_get_words_of_body(root_row):
    r = get_words_of_body(root_row)
    r2 = tests_elements.counter_words_body_row_1()
    assert r == r2


def test_top10_mapper(root_xml):
    r = top10_mapper(root_xml)
    r_impure = tests_elements.list_counters_impure_words_bodys()
    r_pure = tests_elements.list_counters_words_bodys()
    assert r != r_impure and r == r_pure

# Test tarea 2  reduce
def test_top10_reducer(counter1: Counter, counter2: Counter):
    b = top10_reducer(counter1,counter2)
    a = Counter("holahola12")
    assert a == b

def test_reducer_counters(lista_de_counters):
    a = reducer_counters(lista_de_counters)
    b = Counter("holaholas12")       ######
    return a == b

# Test tarea 3 reduce
def test_concat_df_question_or_nan(
                                    dataframe_with_creation_date_q,
                                    dataframe_with_creation_date_a
                                    ):
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

    r = concat_df_answer_or_nan(
                                dataframe_with_creation_date_q,
                                dataframe_with_creation_date_a
                                )
    r2 = dataframe_with_creation_date_a
    assert r.equals(r2)


def test_df_answer(lista_dfs, dataframe_with_creation_date_a):
    r = df_answer(lista_dfs)
    r2 = dataframe_with_creation_date_a
    assert r.equals(r2) 


def test_df_question(lista_dfs,dataframe_with_creation_date_q):
    r = df_question(lista_dfs)
    r2 = dataframe_with_creation_date_q
    assert r.equals(r2)

# Test mapper tarea 3
def test_get_df(root_row):
    r = get_df(root_row)
    rr = get_df(root_row_pos(3))
    r2 = tests_elements.df_question()
    rr2 = tests_elements.df_answer()
    
    assert rr.equals(rr2) and r.equals(r2)  

def test_mapper_averege_time_response(list_two_rows_root):
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
def counter1():
    counter_test = Counter("hola1")
    return counter_test

@pytest.fixture
def counter2():
    counter_test1 = Counter("hola2")
    return counter_test1

@pytest.fixture
def lista_de_counters():
    lista = [Counter("hola1"),Counter("hola2")]
    return lista

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
