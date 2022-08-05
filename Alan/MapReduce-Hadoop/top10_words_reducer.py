
import re
from functools import reduce
from collections import Counter
import sys

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

# Reducers

#reduce la listas de listas de counters a una lista counter
redu = list(map(reducer_counters, sys.stdin()))

#reduce la lista de counters a un solo counter con todas las palabras y sus numero de repeticiones
conter_gigante = reduce(reducer, redu)

# Se obtienen las primeras 10 palabras mas comunes
top10_palabras_mas_nombradas = conter_gigante.most_common(10)

# Top 10 palabras mas comunes
print(top10_palabras_mas_nombradas)