
import sys
from functools import reduce

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

if __name__ == "__main__":
    # Reducer recibe lista de lista de counters con (fecha:str , cantidad_de_repeticiones:int)
    reduced = reduce(reducer, sys.stdin())

    # Se toman los items del Counter ordenandolos de manera acendente 
    # y tomando los primeros 10
    dates_posted = sorted(reduced.items(), key=lambda items: items[1], reverse=False)[0:10]

    # Devuelve las 10 fechas con la menor cantidad de posts
    # [(fecha:str,cantidad_de_post:int)]
    print(dates_posted)