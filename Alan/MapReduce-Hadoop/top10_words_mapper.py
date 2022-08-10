
import re
from pathlib import Path
import xml.etree.ElementTree as ET
from functools import reduce
from collections import Counter
import fileinput

def get_words_of_body(row):
    """
    Takes a chunk's row and gets the value of "Body" attribute, and gets only the words
    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    collections.Counter
        returns words count for  attribute "Body"
    """

    # Obtengo el texto
    date_time = row.get("Body")

    # Se suplanta caracteres que no sean letras por whitespaces
    p = re.compile(r"\W\d")
    results = p.sub(" ", date_time)

    # Separo el texto en palabras
    results = results.split()

    # Se cuentan las palabras repetidas
    results = Counter(results)

    return results

def mapper(chunk):

    """
    Takes a chunk of data and counts words from attribute "Body"

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    list
        returns a list with "Counter´s" of words
    """
    
    list_counter_words = list(map(get_words_of_body , chunk))
    
    return list_counter_words
    
if __name__ == "__main__":
    # Se parsea el .xml file recibido usando "fileinput"
    tree = ET.parse(fileinput.input())
    data = tree.getroot()

    # Chunkify data
    chunk_len = 50
    chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]

    # Maper
    mapped = list(map(mapper, chunks))

    print(mapped)