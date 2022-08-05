"""
    Task [OT254-156]
    This module implements the reducer function of relation
"""
import pandas as pd
import sys

def reducer_top_10_questions():
    """
    Reduces the data returning the top 10 questions.

    Args:
        data (array): The data to reduce.

    Returns:
        top_10_questions (array): The top 10 questions.
    """
    data = []
    for line in sys.stdin:
       #split the line into a list
       line = line.split('\t')
       #append the list to the list
       data.append(line)

    df = pd.DataFrame(data)
    df.columns = ['Id', 'Time']
    df = df.sort_values(by='Time', ascending=False)
    df = df.head(10)
    print(df)

if __name__ == '__main__':
    reducer_top_10_questions()