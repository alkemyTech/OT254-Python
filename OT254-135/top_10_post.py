import xml.etree.ElementTree as ET
from datetime import datetime

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
    return sorted(ar, key=lambda k: k[0])

def reducer(data , key_attrib, value_attrib):
    """Reducer function

    Args:
        data (list): The data to be reduced
        key_attrib (str): The key attribute
        value_attrib (str): The value attribute

    Returns:
        list: The reduced data
    """
    # Create a new list 
    ar = []
    # Current key
    current_post_type_id = None

    current_count = 0

    # Iterate over the data
    for line in data:
        # If the key is none
        if current_post_type_id is None:
            current_post_type_id = line[0]
            current_count = line[1]
        
        # If the key is the same as the current key
        elif current_post_type_id == line[0]:
            current_count += line[1]

        # If the key is different from the current key
        else:
            ar.append({key_attrib: current_post_type_id, value_attrib: current_count})
            current_post_type_id = line[0]
            current_count = line[1]

    ar.append({key_attrib: current_post_type_id, value_attrib: current_count})
    #top 10
    ar = sorted(ar, key=lambda k: k[value_attrib], reverse=True)[:10]
    return ar

if __name__ == '__main__':

    start_time = datetime.now()
    #Read the file
    read_file = read_file('posts.xml')

    #Top 10 tipo de post sin respuestas aceptadas file 1
    maper = maper(read_file)
    final_time = datetime.now()
    print(list(reducer(maper, 'ViewCount', 'Count')))
    print("Time: ", final_time - start_time)