import pandas as pd

def mapper(path: str):
    """This function takes the path of the .xml file with the posts of StackOverflow meta info, parses it into a dataframe and then 
    replace the value of the (text) Body for its lenght.
    
    Args:
        path (str): path of the file in your local drive.
        
    Returns:
        df (dataframe object): dataframe with the metadata of all Stackoverflow posts.
    """
    # Creates dataframe
    df = pd.read_xml(path)

    # Change the value of body field for its length (words count)
    for i, row in df.iterrows():
        df.at[i, 'Body'] = len(row['Body'].split())
        
    return df

def reducer(df):
    """This function takes a dataframe with the metadata of all Stackoverflow posts, adn calculates the correlation
    between the lenth of the body pf the post and its score.

    Args:
        df (dataframe object): dataframe with the metadata of all Stackoverflow posts.

    Returns:
        corr (float): correlation of columns, excluding NA/null values. 

"""
    # Calculates the correlation between posts words (count) and post score
    corr = df['Body'].astype(int).corr(df['Score'])
    
    return corr
    
if __name__ == '__main__':
    path="/home/jmsiro/Desktop/Alkemy/Sprint_3/Posts.xml"
    df = mapper(path)
    words = reducer(df)
    print(words)