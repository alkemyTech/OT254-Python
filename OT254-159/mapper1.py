"""
    Task [OT254-156]
    This module implements the mapper function of get top 10 post.
"""
import sys
import xml.etree.ElementTree as ET

def read_file(file_name):
    """Read the file and return the root

    Args:
        file_name (str): The file name

    Returns:
        xml.etree.ElementTree.Element: The root of the xml file
    """
    tree = ET.parse(file_name)
    return tree.getroot()

def maper(data):
    """Mapper function

    Args:
        data (list): The data to be mapped

    Returns:
        list: The mapped data
    """
    ar = []
    # Iterate over the data
    for line in data.findall('row'):
        # If the attribute is in the row
        if 'AcceptedAnswerId' not in line.attrib:
            # Create a dictionary
            ar.append([line.attrib['ViewCount'], 1])
    #Sort array by key_attrib
    v = sorted(ar, key=lambda k: k[0])
    for i in range(len(v)):
        print('%s\t%s' % (v[i][0], v[i][1]))

if __name__ == '__main__':

    #Read the file
    read_file = read_file(sys.stdin)

    #Top 10 tipo de post sin respuestas aceptadas file 1
    maper = maper(read_file)