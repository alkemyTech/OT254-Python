import pandas as pd

def mapper(path_p: str, path_c: str):
    """This function takes the paths of two .xml files with the posts of StackOverflow  meta info and another with its coments, parses them into dataframes 
    and then change from string to datetime the date fields and obtain the firt comment made over each comment.

    Args:
        path_p (str): path of the posts metadata file in your local drive.
        path_c (str): path of the comments metadata file in your local drive.

    Returns:
        result (list): List of two dataframes.
    """
    result = []
    
    df_p = pd.read_xml(path_p)
    df_c = pd.read_xml(path_c)
    
    # Str to Datetime format
    df_p['CreationDate'] = pd.to_datetime(df_p['CreationDate'])
    result.append(df_p)
    
    # Str to Datetime format
    df_c['CreationDate'] = pd.to_datetime(df_c['CreationDate'])
    # Get first comment of each post
    df_c = df_c[df_c['CreationDate'] == df_c.groupby('PostId')['CreationDate'].transform('min')]
    result.append(df_c)
    
    return result

def reducer(dfs: list):
    """This function takes two dataframes, takes the needed column fields of each one and merge them to 
    calculate the average time that between the post date and the first comment (over it) date.

    Args:
        dfs (list): List of two detaframes.

    Returns:
        result (datetime object): Average time between the Post Date and the First Comment Date.
    """
    # Copy Columns needed into new dataframe
    df_p_ = dfs[0][['Id', 'CreationDate']].copy()
    # Remane Columns
    df_p_ = df_p_.rename(columns={"Id":"PostId", "CreationDate":"PostCreation"})
   
    # Copy Columns needed into new dataframe
    df_c_ = dfs[1][['PostId', 'CreationDate']].copy()
    # Remane Columns
    df_c_ = df_c_.rename(columns={"CreationDate":"CommentCreation"})
    
    # Combine new dataframes
    result = pd.merge(df_p_, df_c_, on="PostId")
    
    # Calculate delay between post date and first comment date
    result['Delay'] = result.CommentCreation - result.PostCreation
    
    return result['Delay'].mean()
    
if __name__ == '__main__':
    path_p="/home/jmsiro/Desktop/Alkemy/Sprint_3/Posts.xml"
    path_c="/home/jmsiro/Desktop/Alkemy/Sprint_3/Comments.xml"
    dfs = mapper(path_p, path_c)
    average = reducer(dfs)
    print(average)