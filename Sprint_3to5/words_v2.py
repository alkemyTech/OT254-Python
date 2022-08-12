"""
Processes and analyses StackOverflow data to obtain the top 10 of the least viewed posts.
"""
from xmlparser import get_data, chunkify
import logging.config
import pandas as pd

# Loads logs configuration using .cfg file
logging.config.fileConfig('/home/jmsiro/Desktop/Alkemy/Sprint_3/log_bd.cfg')
logger = logging.getLogger(__name__)

def words_score(row):
    """
    Takes a chunk's row, calculates the quantity of words of the "Body" attribute and gets the "Score" attribute.

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    list
        returns a pair of values, quentity of words of the "Body" and the "Score" attribute of the post
    """
    data = []
    words = len(row.get("Body").replace(".", "").replace(",", "").split())
    data.append(words)
    score = int(row.get("Score"))
    data.append(score)

    return data
    
def mapper(chunk):

    """
    Takes a chunk of data and build a list of lists with the value pair quantity of words of the "Body"and "Score".

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    list
        returns a list of matrixes (two columns: Body and Score)
    """
    
    cor = list(map(words_score, chunk))
    
    return cor
    

def reducer(data):
    """
    Takes a list of lists and transformes it into a dataframe to calculate the correlation between the given data.

    Parameters
    ----------
    data : list
        List of lists with the couple quantity of words in the "Body" attribute and "Score" attribute.

    Returns
    -------
    float
        returns the result of calculating the correlation between quantity of words and "Score" of each post.
    """
    # Iterates over de the lists to create a unique list of lists
    data_ =[]
    for list in data:
        for pair in list:
            data_.append(pair)
    # Creates a dataframe with that unique list
    df = pd.DataFrame(data_, columns = ["Body", "Score"])
    
    corr = df['Body'].corr(df['Score'])
    
    return corr

if __name__ == '__main__':
    try:
        # Parses the .xml file
        logger.info('Starting to parse .xml file...')
        data = get_data('/home/jmsiro/Desktop/Alkemy/Sprint_3/tests_data/Posts_test.xml')
    except FileNotFoundError as e:
        pass
        logger.error('Something went wrong..')
        logger.error(e)
    else:
        # Data splitter
        logger.info('Starting to split data...')
        data_chunks = chunkify(data, 50)
        
        # Main mapping function
        logger.info('Starting to map data...')
        mapped = list(map(mapper, data_chunks))
        
        # Main reduction function
        logger.info('Starting to reduce data...')
        reduced = reducer(mapped)

        # Correlation
        logger.info(f"The correlation between quantity of words and the score of each post is: {reduced}")   