import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd

def read_file(file_name):
    """
    Reads the file and returns the root of the tree.
    Args:
        file_name (str): The name of the file to read.

    Returns:
        root (Element): The root of the tree.
    """
    tree = ET.parse(file_name)
    return tree.getroot()

def maper(data):
    """
    Maps the data to a list of lists.
    Args:
        data (Element): The data to map.

    Returns:
        ar (array): The mapped data.
    """
    ar = []
    for line in data.findall('row'):
        if line.attrib['PostTypeId'] == '2':
            activity = datetime.strptime(line.attrib['LastActivityDate'], '%Y-%m-%dT%H:%M:%S.%f')
            creation = datetime.strptime(line.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')
            ar.append([line.attrib['Id'], (activity - creation).total_seconds()])
    return ar

def reducer_top_10_questions(data):
    """
    Reduces the data returning the top 10 questions.

    Args:
        data (array): The data to reduce.

    Returns:
        top_10_questions (array): The top 10 questions.
    """
    df = pd.DataFrame(data)
    df.columns = ['Id', 'Time']
    df = df.sort_values(by='Time', ascending=False)
    df = df.head(10)
    return df

if __name__ == '__main__':
    #Read the file
    read_file = read_file('posts.xml')
    #Map the data
    data = maper(read_file)
    #Reduce the data
    data = reducer_top_10_questions(data)
    print(data)
