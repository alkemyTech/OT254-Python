"""
    Task [OT254-156]
    This module implements the reducer function of get top 10 post.
"""
import pandas as pd 
import sys

def reducer_relation():
    """
    Reduces the data returning the relation.

    Args:
        data (array): The data to reduce.

    Returns:
        relation (array): The relation.
    """
    
    #create a list to store the data
    ar = []
    #loop through the data
    for line in sys.stdin:
        #split the line into a list
        line = line.strip().split('\t')
        #append the list to the list
        ar.append(line)

    #create a dataframe from the list
    df = pd.DataFrame(ar)
    #rename the columns
    df.columns = ['AnswerCount', 'Score']
    #calculate the correlation
    relation = df.corr()
    print(relation)

if __name__ == '__main__':
    reducer_relation()
