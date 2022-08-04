"""
Processes and analyses StackOverflow data to obtain the top 10 of the least viewed posts.
"""
from parser import get_data, chunkify
import logging.config
from functools import reduce
from collections import Counter

# Loads logs configuration using .cfg file
logging.config.fileConfig('/home/jmsiro/Desktop/Alkemy/Sprint_3/log_bd.cfg')
logger = logging.getLogger(__name__)

def get_tags(row):
    """
    Takes a chunk's row, checks if "AcceptedAnswerId" attribute exists and if it is greater than cero, and if True, 
    it gets the "Tag" attribute and separete the string if more than one.

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    list
        returns used tag(s) in the post
    """
    if row.get("AcceptedAnswerId") and int(row.get("AcceptedAnswerId")) > 0:
        tags = row.get("Tags")
        tags = tags.replace("<", " ").replace(">", " ").split()
        return tags
    else:
        pass
    
def mapper(chunk):

    """
    Takes a chunk of data and build a list with every used tag.

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    collections.Counter
        returns occurrences count for attribute "Tag"
    """

    tags = list(map(get_tags, chunk))
    
    tags_ = []
    for tag in tags:
        # Eliminate posts with "AcceptedAnswerId" attribute > 0 that has no tag(s)
        if tag == None:
            pass
        # If the post has more than one tag it separetes them, as different occurences, beffor appending to tags list of the chunk
        elif isinstance(tag, list):
            for t in tag:
                tags_.append(t)
        else:
             # If the post has one tag it append it to the list directly
            tags_.append(tag)
    
    return Counter(tags_)

def reducer(count1, count2):
    """
    Takes occurrences count for attribute "Tag" from chunks and updates count from first chunk

    Parameters
    ----------
    count1 : collections.Counter
        Counter to be update

    count2 : collections.Counter
        Counter used to do updates

    Returns
    -------
    collections.Counter
        returns updated counter
    """

    count1.update(count2)

    return count1

if __name__ == '__main__':
    try:
        # Parses the .xml file
        logger.info('Starting to parse .xml file...')
        data = get_data('Posts.xml')
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
        reduced = reduce(reducer, mapped)

        # Top 10 tags used in posts with accepted answers
        top10= reduced.most_common(10)
        logger.info(f"Top 10 tags used in posts with accepted answers: {top10}")
    