"""
Processes and analyses StackOverflow data to obtain the top 10 of the least viewed posts.
"""
from parser import get_data, chunkify
import logging.config
import pandas as pd

# Loads logs configuration using .cfg file
logging.config.fileConfig('/home/jmsiro/Desktop/Alkemy/Sprint_3/log_bd.cfg')
logger = logging.getLogger(__name__)

def get_col_p(row):
    """
    Takes a chunk's row and creates a list with "Id" and "CreationDate" attributes

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    list
        returns post Id and creation date
    """
    list_ = []    
    list_.append(row.get("Id"))
    list_.append(row.get("CreationDate"))

    return list_

def get_col_c(row):
    """
    Takes a chunk's row and creates a list with "PostId" (over which the comment was made) and "CreationDate" attributes

    Parameters
    ----------
    row : xml.etree.ElementTree.Element
        Chunk's row

    Returns
    -------
    list
        returns post Id ans creation date
    """    
    
    list_ = []
    list_.append(row.get("PostId"))
    list_.append(row.get("CreationDate"))
    
    return list_
    
def mapper_p(chunk):

    """
    Takes a chunk of data and build a list of lists with the the value pair "Id" and "CreationDate".

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    collections.Counter
        returns a list of matrixes (two columns: Id and CreationDate)
    """

    posts = list(map(get_col_p, chunk))
    
    return posts

def mapper_c(chunk):

    """
    Takes a chunk of data and build a list of lists with the the value pair "PostId" and "CreationDate".

    Parameters
    ----------
    chunk : xml.etree.ElementTree.Element
        Slice of data from the entire .xml file

    Returns
    -------
    collections.Counter
        returns a list of matrixes (two columns: PostId and CreationDate)
    """

    comments = list(map(get_col_c, chunk))
    
    return comments

def reducer(mapped_p, mapped_c):
    """
    Takes two list of lists and transformes them into a dataframes, combines them and determines the delay between the post creation date
    and the firt comment creation date.

    Parameters
    ----------
    mapped_p : list
        List of lists of values to be converted into a dataframe

    mapped_c : list
        List of lists of values to be converted into a dataframe

    Returns
    -------
    floeat
        returns the average delay between the post creation date and the firt comment creation date.
    """
    # Iterates over de the lists to create a unique list of lists
    data_p =[]
    for list in mapped_p:
        for pair in list:
            data_p.append(pair)
    # Creates a dataframe with that unique list
    df_p = pd.DataFrame(data_p, columns = ["PostId", "PostCreation"])
    df_p['PostCreation'] = pd.to_datetime(df_p['PostCreation'])
    
    # Iterates over de the lists to create a unique list of lists
    data_c =[]
    for list in mapped_c:
        for pair in list:
            data_c.append(pair)
    # Creates a dataframe with that unique list
    df_c = pd.DataFrame(data_c, columns = ["PostId", "CommentCreation"])
    df_c['CommentCreation'] = pd.to_datetime(df_c['CommentCreation'])
    # Eliminates all occurences of "PostId", but the one with the minimun date
    df_c = df_c[df_c["CommentCreation"] == df_c.groupby("PostId")["CommentCreation"].transform("min")]
    
    # Combine new dataframes
    result = pd.merge(df_p, df_c, on="PostId")
    
    # Calculate delay between post date and first comment date
    result["Delay"] = result.CommentCreation - result.PostCreation
    
    return result["Delay"].mean()

if __name__ == '__main__':
    try:
        # Parses the .xml file
        logger.info('Starting to parse .xml files...')
        data_p = get_data('Posts.xml')
        data_c = get_data('Comments.xml')
    except FileNotFoundError as e:
        pass
        logger.error('Something went wrong..')
        logger.error(e)
    else:
        # Data splitter
        logger.info('Starting to split data...')
        data_chunks_p = chunkify(data_p, 50)
        data_chunks_c = chunkify(data_c, 50)
        
        # Main mapping function
        logger.info('Starting to map data...')
        mapped_p = list(map(mapper_p, data_chunks_p))
        mapped_c = list(map(mapper_c, data_chunks_c))

        # Main reduction function
        logger.info('Starting to reduce data...')
        reduced = reducer(mapped_p, mapped_c)

        # Average delay between the post creation date and the firt comment creation date. 
        logger.info(f"The average delay between the post creation date and the firt comment creation date is: {reduced}")
    