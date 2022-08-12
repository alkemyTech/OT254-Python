import pandas as pd

def mapper(path: str):
    """This function takes the path of the .xml file with the posts of StackOverflow meta info, parses it into a dataframe and then filter 
    the posts with accepted answers and return the tags used in those posts.
    
    Args:
        path (str): path of the file in your local drive.

    Returns:
        tags (list): collection of all used tags in posts with acceptes answers.
    """
    # Creates dataframe
    df = pd.read_xml(path)
    # New DF with posts wit at least one Accepted Answer
    df_accepted = df[df['AcceptedAnswerId'] > 0]
    # Create list with the tags used
    tags = df_accepted['Tags'].to_list()
    
    return tags

def reducer(tags: list):
    """This function takes a list with the tags used in each post, parses it into a list of lists 
    and creates a dictionary with the tag as key and the number of appearances as value.

    Args:
        tags (list): list of the tags used in each post with accepted answers.

    Returns:
        result (list): list of the top 10 used tags. List of toupels (str, int) -> ('tag', number of appearences).
    """
    data = {}
    
    # Iterates over list of tags
    for tag in tags:
        # Change tags format from a single string to a list
        values = tag.replace("<", " ").replace(">", " ").split()
        # Iterates through the list of tags and count them, adding the data into a dictionarie (k: tag, v: count)
        for value in values:
            if value in data:
                data[value] += 1
            else:
                data[value] = 1
    
    # Order dictionarie for value in descending order             
    data_ = {k: v for k, v in sorted(data.items(), key=lambda item: item[1], reverse=True)}
    
    # Creates ist with the 10 moste used ones
    i = 0
    result = []
    for item in data_.items():
        if i < 10:
            result.append(item)
        i += 1
    return result
    
if __name__ == '__main__':
    path="/home/jmsiro/Desktop/Alkemy/Sprint_3/Posts.xml"
    tags = mapper(path)
    top10 = reducer(tags)