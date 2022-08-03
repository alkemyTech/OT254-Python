import xml.etree.ElementTree as ET
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
        data (array): The data to map.

    Returns:
        ar (list): The mapped data.
    """
    ar = []
    for line in data.findall('row'):
        if 'AnswerCount' in line.attrib:
            ar.append([int(line.attrib['AnswerCount']),int(line.attrib['Score'])])
    return ar

def reducer_relation(data):
    """
    Reduces the data returning the relation.

    Args:
        data (array): The data to reduce.

    Returns:
        relation (array): The relation.
    """
    df = pd.DataFrame(data)
    df.columns = ['AnswerCount', 'Score']
    relation = df.corr()
    return relation

if __name__ == '__main__':
    #Read the file
    read_file = read_file('posts.xml')
    #Map the data
    data = maper(read_file)
    #Reduce the data
    data = reducer_relation(data)
    print(data)