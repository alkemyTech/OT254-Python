
from pathlib import Path
import xml.etree.ElementTree as ET
from functools import reduce
from collections import Counter

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


def reducer(count1, count2):

    """
    Takes occurrences count for attribute "CreationDate" from chunks and updates count from first chunk

    Parameters
    ----------
    count1 : collections.Counter
        Counter to be update

    count2 : collections.Counter
        Counter used to do updates

    Returns
    -------
    collections.Counter
        returns updated counter
    """

    count1.update(count2)

    return count1


if __name__ == '__main__':

    # Path file
    path_file = Path.joinpath(Path.cwd().parent.parent, "Desktop","112010 Stack Overflow","posts.xml" )

    try:

        # Se parsea el .xml file
        tree = ET.parse(path_file)
        data = tree.getroot()

    except FileNotFoundError as e:
        print("err")

    else:
        
        # Chunkify data
        chunk_len = 50
        chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]

        # Maper principal
        mapped = list(map(mapper, chunks))

        # Reducer principal
        reduced = reduce(reducer, mapped)

        # Se toman los items del Counter ordenandolos de manera acendente 
        # y tomando los primeros 10
        """
        sorted() ---> devuelve una lista ordenando objetos iterables de menor a mayor
        reduced.items() ---> expresa los items del Counter como diccionario

        key = lambda pair: pair[1] ---> funcion que toma los values del diccionario en cada iteracion 
        pasandoselos a la funcion sorted() que toma como parametro a ordenar

        reverse = False ---> pongo explicitamente que no revierta el ordenamiento original acendente de la funcion
        """
        dates_posted = sorted(reduced.items(), key=lambda items: items[1], reverse=False)[0:10]

        print(f"Top 10 dates with the fewest number of posts created: {dates_posted}")
        