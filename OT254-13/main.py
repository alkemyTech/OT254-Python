import pandas as pd
from decouple import config


#se lee el archivo sql correspondiente a la univ. nacional del comahue
file = open('Univ_Nacional_Del_Comahue.sql','r' )
Univ_Nacional_Del_Comahue_sql = file.read()

#se lee el archivo sql correspondiente a la univ. del salvador
file = open('Universidad_Del_Salvador.sql','r' )
Universidad_Del_Salvador_sql = file.read()


#variavle para controlar la cantidad de errores producidos
error_control = 0

while error_control < 5:
    try:
        #Se ejecutan laa consultas sql correspondientes a cada universidad y se genera un dataframe para cada una.
        df_unv_Comahue = pd.read_sql(Univ_Nacional_Del_Comahue_sql, config('DB_DATA_CONNECT'))
        df_unv_del_salvador = pd.read_sql(Universidad_Del_Salvador_sql, config('DB_DATA_CONNECT'))
        #se crea un dataframe con el archivo auxiliar de codigos postales
        df_codigos_postales = pd.read_csv('codigos_postales.csv')       
        #se une el df con los codigos postales con el dataframe de la universidad del salvador
        df_unv_del_salvador = pd.merge(df_unv_del_salvador,df_codigos_postales, how = 'left')
        #se remove la columna localidad del df de la universidad del salvador ya que no es requerida
        df_unv_del_salvador = df_unv_del_salvador.drop(['localidad'], axis=1)
        #Se imprimen los df en pantalla
        print(df_unv_Comahue)
        print(df_unv_del_salvador)
        break
    except Exception as error:
        # se controla si se han producido 5 errores antes de imprimir el error
        if error_control == 4:
            #se imprime el error
            print(error)
            break
        else:
            #se incrementa la variavle con la que se controla el error    
            error_control = error_control + 1
            
