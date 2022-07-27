from sqlalchemy import create_engine
import pandas as pd
from decouple import configs

def extraer(universidad, path_sql, path_files):

    try:
        engine = create_engine(config("DB_CONNECTION"))
        engine.connect()
    except:
        return "error al conectar con la base de datos"
    else:
        query = open(path_sql + universidad + ".sql" ,"r")

        df = pd.read_sql_query(query.read(), config("DB_CONNECTION"))
    
        df.to_csv(path_files + universidad +".csv", index=None)
