import pandas as pd
from decouple import config


def procesar_unv_del_salvador():    
    #se lee el archvio sql correspondiente a la universidad del salvador  
    file = open('Universidad_Del_Salvador.sql', 'r')
    universidad_del_salvador_sql = file.read()
   
    #Se ejecutan laa consulta sql y se genera un dataframe .
    df_unv_del_salvador = pd.read_sql(universidad_del_salvador_sql, config('DB_DATA_CONNECT'))

    #se convierte el texo a minusculas de todas las columnas.
    df_unv_del_salvador = df_unv_del_salvador.astype(str).apply(lambda x: x.str.lower())

    #se renombran las columnas necesarias
    df_unv_del_salvador.rename(columns={          
        'universidad':'university',          
        'carrera' : 'career',              
        'sexo' : 'gender',                 
        'codigo_postal' : 'postal_code',        
        'direccion' : 'location',            
        'correo_electronico' : 'email'
        },inplace=True)

   
    #se crea un dataframe con el archivo auxiliar de codigos postales
    df_codigos_postales = pd.read_csv('codigos_postales.csv')       
    #se une el df con los codigos postales con el dataframe de la universidad del salvador
    df_unv_del_salvador = pd.merge(df_unv_del_salvador,df_codigos_postales, how = 'left')
    #se remuve la columna localidad del df de la universidad del salvador ya que no es requerida
    df_unv_del_salvador = df_unv_del_salvador.drop(['localidad'], axis=1)

    #se remplazan los _ por espacios.
    df_unv_del_salvador["university"]= df_unv_del_salvador["university"].str.replace('_', ' ')
    df_unv_del_salvador["career"]= df_unv_del_salvador["career"].str.replace('_', ' ')
    df_unv_del_salvador["location"]= df_unv_del_salvador["location"].str.replace('_', ' ')

    #se exportan los datos en un archivo txt
    df_unv_del_salvador.to_csv('unv_del_salvador.txt')


def procesar_unv_Comahue():
     #se lee el archvio sql correspondiente a la universidad nacional de comahue.
    file = open('Univ_Nacional_Del_Comahue.sql', 'r')
    univ_nacional_del_comahue_sql = file.read()

    #Se ejecutan laa consulta sql y se genera un dataframe .
    df_unv_Comahue = pd.read_sql(univ_nacional_del_comahue_sql, config('DB_DATA_CONNECT'))

    #se renombran las columnas necesarias
    df_unv_Comahue.rename(columns={          
        'universidad':'university',          
        'carrera' : 'career',              
        'fecha_de_inscripcion' : 'inscription_date', 
        'sexo' : 'gender',                 
        'codigo_postal' : 'postal_code',        
        'direccion' : 'location',            
        'correo_electronico' : 'email'
        },inplace=True)

    #se convierte el texo a minusculas de todas las columnas.
    df_unv_Comahue = df_unv_Comahue.astype(str).apply(lambda x: x.str.lower())
    #se exportan los datos en un archivo txt
    df_unv_Comahue.to_csv('unv_Comahue.txt')




