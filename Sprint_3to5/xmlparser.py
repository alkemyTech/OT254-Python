from logging import exception
import xml.etree.ElementTree as ET

class TypeException(Exception):
    pass

def get_data(file):
    """
    Takes a .xml file and parses it

    Parameters
    ----------
    file : str
        Name of the .xml file with posts data from StackOverflow

    Returns
    -------
    xml.etree.ElementTree.Element
        Returns parsed file
    """
    if file.lower().endswith('.xml'):
        tree = ET.parse(file)
        root = tree.getroot()
        return root
    else:
        raise TypeException('Error with file extension, must be .xml')

def chunkify(data, chunk_len):
    """
    Takes the parsed .xml file and splits it in n equal chunks

    Parameters
    ----------
    data : xml.etree.ElementTree.Element
        Parsed .xml file

    chunk_len : int
        Chunk's length

    Returns
    -------
    list
        returns list of chunks
    """

    chunks = [data[i:i + chunk_len] for i in range(0, len(data), chunk_len)]
    return chunks
