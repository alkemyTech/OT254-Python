
"""
    An ET.Element is created along with other elements to test
    inputs and outputs in tests.py
"""

import pandas as pd
from collections import Counter
import xml.etree.ElementTree as ET



def root():
    """
    Create five sub elements in an xml.etree.ElementTree.Element
    and finally returning it

    Return:
    ------
        xml.etree.ElementTree.Element
    """
    root = ET.Element("posts")
    ET.SubElement(
                    root, "row",
                    Id="0",
                    PostTypeId="1",
                    CreationDate="2009-03-11T12:51:01.480",
                    Body="lorem lorem",
                    Score="50"
        )
    ET.SubElement(
                    root, "row",
                    Id="1",
                    PostTypeId="1",
                    CreationDate="2009-03-11T12:51:01.480",
                    Body="lorem lo rem",
                    Score="200"
    )
    ET.SubElement(
                    root, "row",
                    Id="2",
                    PostTypeId="1",
                    CreationDate="2009-03-11T12:51:01.480",
                    Body="lorem lorem",Score="150"
    )
    ET.SubElement(
                    root, "row",
                    Id="3",
                    PostTypeId="2",
                    CreationDate="2009-03-11T12:51:01.480",
                    ParentID="2",
                    Body="lorem lorem",
                    Score="100"
    )
    ET.SubElement(
                    root, "row",
                    Id="4",
                    PostTypeId="2",
                    CreationDate="2009-03-11T12:51:01.480",
                    ParentID="1",
                    Body="lorem lorem+1",
                    Score="600"
    )
    return root


def counter_creationdate():
    """
    returns a Counter of five dates

    Return:
    ------
        Counter[str]
    """
    return Counter(
        [
            root()[0].get("CreationDate").split('T')[0],
            root()[1].get("CreationDate").split('T')[0],
            root()[2].get("CreationDate").split('T')[0],
            root()[3].get("CreationDate").split('T')[0],
            root()[4].get("CreationDate").split('T')[0],
        ]
    )

def counter_words_body_row_1():
    """
    returns a Counter of three words

    Returns:
    -------

       Counter[str]
    """
    return Counter(
        [
            "lorem",
            "lo",
            "rem",
        ]
    )

def list_counters_impure_words_bodys():
    """
    eturns a list of word counters with
    special characters and numbers

    Return:
    ------

        List [ Counter [str] ]
    """
    return [
            Counter(root()[0].get("Body").split()),
            Counter(root()[1].get("Body").split()),
            Counter(root()[2].get("Body").split()),
            Counter(root()[3].get("Body").split()),
            Counter(root()[4].get("Body").split()),
        ]
def list_counters_words_bodys():
    """
    returns a list of word counters without
    special characters or numbers

    Return:
    -------

        List[Counter[str]]

        
    """
    return [
            Counter(root()[0].get("Body").split()),
            Counter(root()[1].get("Body").split()),
            Counter(root()[2].get("Body").split()),
            Counter(root()[3].get("Body").split()),
            Counter(["lorem","lorem"]),
        ]

def df_question():
    """
    returns a Pandas.Dataframe

    Return:
    ------

        Dataframe: columns = id,score,creation_date_q
    """
    return  pd.DataFrame({
                        "id":[int(root()[1].get("Id"))],
                        'score':[int(root()[1].get("Score"))],
                        "creation_date_q":[root()[1].get("CreationDate").split('T')[0]],
                        })

def df_answer():
    """
    returns a Pandas.Dataframe
    Return:
    ------

        Dataframe: columns = creation_date_a,id
    """
    return  pd.DataFrame({
                        "creation_date_a":[root()[3].get("CreationDate").split('T')[0]],
                        "id":[int(root()[3].get("ParentID"))],
                        })

def lista_dfs():
    """
    returns a list with two Pandas.Dataframes

    Return:
    ------

        Dataframe_1: columns = id,score,creation_date_q
        Dataframe_2: columns = id,score,creation_date_q
    """
    return  [
        pd.DataFrame({
                        "id":[int(root()[1].get("Id"))],
                        'score':[int(root()[1].get("Score"))],
                        "creation_date_q":[root()[1].get("CreationDate").split('T')[0]],
                    }),
        pd.DataFrame({
                        "id":[int(root()[2].get("Id"))],
                        'score':[int(root()[2].get("Score"))],
                        "creation_date_q":[root()[2].get("CreationDate").split('T')[0]],
                    })
    ]
