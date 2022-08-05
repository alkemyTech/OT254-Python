"""
    Task [OT254-156]
    This module implements the mapper function of top 10 average time.
"""
import xml.etree.ElementTree as ET
from datetime import datetime
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
        data (Element): The data to map.

    Returns:
        ar (array): The mapped data.
    """
    ar = []
    for line in data.findall('row'):
        if line.attrib['PostTypeId'] == '2':
            activity = datetime.strptime(line.attrib['LastActivityDate'], '%Y-%m-%dT%H:%M:%S.%f')
            creation = datetime.strptime(line.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')
            print('%s\t%s' % (line.attrib['Id'], (activity - creation).total_seconds()))
    

if __name__ == '__main__':
    #Read the file
    read_file = read_file(sys.stdin)
    #Map the data
    data = maper(read_file)
