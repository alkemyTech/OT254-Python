"""
    Task [OT254-156]
    This module implements the mapper function of relation
"""
import xml.etree.ElementTree as ET
import pandas as pd 
import sys

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
    x = sorted(ar, key=lambda x: x[1], reverse=True)
    for i in range(len(x)):
        print('%s\t%s' % (x[i][0], x[i][1]))

if __name__ == '__main__':
    #Read the file
    read_file = read_file(sys.stdin)
    #Map the data
    data = maper(read_file)
