
import sys

def reducer():
    """Reducer function

    Args:
        data (list): The data to be reduced
        key_attrib (str): The key attribute
        value_attrib (str): The value attribute

    Returns:
        list: The reduced data
    """
    key_attrib= 'ViewCount'
    value_attrib='Count'
    # Create a new list 
    ar = []
    # Current key and count
    current_post_type_id = None
    current_count = 0

    # Iterate over the data
    for line in sys.stdin:
        
        # Line is a list of strings
        line = line.strip().split('\t')

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
    print(ar)

if __name__ == '__main__':
    reducer()