import pandas as pd

def procesar(universidad, path_files, link):
    """
    parametros de entrada:
        universidad: nombre de la universidad
        path_file: ubicacion del archivo universidad.csv
        link: descarga el csv "codigos_psotales.csv"

    carga un csv ubicado en la ruta recivida con el nombre de la 
    universidad y lo une con el csv descargado desde el link recibido.
    Finalmente los datos se normalizan y se guardan en la misma ruta como txt

    """
    #carga de datos
    df = pd.read_csv(path_files + universidad +".csv")
    df_codigos_postales = pd.read_csv(link)

    df_codigos_postales.rename(columns={
                                        'codigo_postal' : 'postal_code',
                                        'localidad' : 'location'
                                        }, inplace= True)

    df.rename(columns={
                        'localidad' : 'location',
                        'edad' : 'age'
                        }, inplace= True)

    df_codigos_postales['location'] = df_codigos_postales['location'].str.lower().replace({'-' : ' '},regex=True)

    #Se hace el merge dependiendo el caso
    if "location" in df.columns and "location" in df_codigos_postales.columns:  
        df['location'] = df['location'].str.lower().replace({'-' : ' '},regex=True)
        df = df.merge(df_codigos_postales, on='location',how='left')
    elif "postal_code" in df.columns and "postal_code" in df_codigos_postales.columns:
        df = df.merge(df_codigos_postales, on='postal_code',how='left')
    #Se normalizan los datos
    df['age'] = df['age']/365-1
    df['career'] = df['career'].str.lower().replace({'-' : ' '},regex=True)
    df['university'] = df['university'].str.lower().replace({'-' : ' '},regex=True)
    df['postal_code'] = df['postal_code'].apply(str)
    df['first_name'] = df['first_name'].str.lower().replace({'-' : ' '},regex=True)
    df['last_name'] = df['last_name'].str.lower().replace({'-' : ' '},regex=True)
    #rio_df['inscription_date'] = pd.to_datetime(rio_df['inscription_date'])
    #rio_df['inscription_date'] =  pd.to_datetime(rio_df['inscription_date'], format='%Y\/%b\/%d')
    df['gender'] = df['gender'].replace({'M':'Male', 'F':'Female'},regex=True)
    
    #Se guarda el txt en el path recibido como parametro
    df.to_csv(path_files + universidad +".txt", index=None)