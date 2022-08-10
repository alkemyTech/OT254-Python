
"""
    Se crea un ET.Element junto con otros elementos para provar
    entradas y salidas en tests.py
"""

import pandas as pd
from collections import Counter
import xml.etree.ElementTree as ET



def root():
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
    return Counter(
        [
            "lorem",
            "lo",
            "rem",
        ]
    )

def list_counters_impure_words_bodys():
    return [
            Counter(root()[0].get("Body").split()),
            Counter(root()[1].get("Body").split()),
            Counter(root()[2].get("Body").split()),
            Counter(root()[3].get("Body").split()),
            Counter(root()[4].get("Body").split()),
        ]
def list_counters_words_bodys():
    return [
            Counter(root()[0].get("Body").split()),
            Counter(root()[1].get("Body").split()),
            Counter(root()[2].get("Body").split()),
            Counter(root()[3].get("Body").split()),
            Counter(["lorem","lorem"]),
        ]

def df_question():
    return  pd.DataFrame({
                        "id":[int(root()[1].get("Id"))],
                        'score':[int(root()[1].get("Score"))],
                        "creation_date_q":[root()[1].get("CreationDate").split('T')[0]],
                        })

def df_answer():
    return  pd.DataFrame({
                        "creation_date_a":[root()[3].get("CreationDate").split('T')[0]],
                        "id":[int(root()[3].get("ParentID"))],
                        })

def lista_dfs():
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
