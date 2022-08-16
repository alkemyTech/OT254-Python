
import re
from pathlib import Path
import xml.etree.ElementTree as ET
from functools import reduce
from collections import Counter

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

    #obtengo el texto
    date_time = row.get("Body")

    #Se suplanta caracteres que no sean letras por whitespaces
    p = re.compile(r"\W")
    results = p.sub(" ", date_time)

    #Separo el texto en palabras
    results = results.split()

    #Se cuentan las palabras repetidas
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
    
    posts_views = list(map(get_words_of_body , chunk))
    
    return posts_views

def reducer_counters(c):
    """
    Takes a list of counters and return a only counter

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    collections.Counter
        returns occurrences count for attribute "Body"
    """

    posts_views = reduce(reducer, c)
    
    return posts_views

def reducer(count1, count2):

    """
    Takes words count for attribute "Body" from chunks and updates count from first chunk

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

        # Maper
        mapped = list(map(mapper, chunks))

        # Reducers

        #reduce los counters de las listas de mapped[n] a uno solo counter por lista
        redu = list(map(reducer_counters, mapped))
        
        #reduce la lista a un solo counter con todas las palabras y sus numero de repeticiones
        conter_gigante = reduce(reducer, redu)

        # Se obtienen las primeras 10 palabras mas comunes
        top10_palabras_mas_nombradas = conter_gigante.most_common(10)

        # Top 10 palabras mas comunes
        print(f"Top 10 most common words: {top10_palabras_mas_nombradas}")