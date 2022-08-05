
import xml.etree.ElementTree as ET
from collections import Counter
import fileinput

def get_date(row):
    """
    Takes a chunk's row and gets the value of "CreationDate" attribute

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------

    str
        returns a string date 
    """
    date_time = row.get("CreationDate")

    return date_time.split('T')[0]


def mapper(chunk):
    """
    Takes a chunk of data and counts occurrences from attribute "CreationDate"

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    collections.Counter
        returns occurrences count for attribute "CreationDate"
    """
    dates = list(map(get_date, chunk))
    
    return Counter(dates)


# Se parsea el .xml file recibido usando "fileinput"
tree = ET.parse(fileinput.input())
data = tree.getroot()

# Chunkify data
chunk_len = 50
chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]

# Maper principal
mapped = list(map(mapper, chunks))

# Devuelve una lista de lista de collections.Counters 
# que contiene (fecha:str , cantidad_de_repeticiones: int)
print(mapped)