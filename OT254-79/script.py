#university: str minúsculas, sin espacios extras, ni guiones
import pandas as pd
from decouple import config

def normalize(string):
    return  string.str.lower().str.replace('  ', ' ').str.replace('_', ' ')

# si es m return male
# si es f return female
def male_or_female(string):
    return string.str.replace('m', 'male').replace('f', 'female')


def solucion_universidad_tres_de_febrero():
    #Leer archivo query1.sql
    sql = pd.read_sql_query(open('query1.sql', 'r').read(), config('CONNECTION_DB'))
    
    #normalizar datos
    sql['university'] = normalize(sql['university'])
    sql['career'] = normalize(sql['career'])
    sql['first_name'] = normalize(sql['first_name'])
    sql['last_name'] = normalize(sql['last_name'])
    sql['gender'] = male_or_female(sql['gender'])
    sql['location'] = normalize(sql['location'])
    sql['email'] = normalize(sql['email'])

    #Generar archivo txt que guarde los datos del sql para
    sql.to_csv('Universidad Nacional de Tres de Febrero')
    
def solucion_universidad_tecnologica_nacional():
    #Leer archivo query2.sql
    sql = pd.read_sql_query(open('query2.sql', 'r').read(), config('CONNECTION_DB'))
    
    #normalizar datos
    #Ya están normalizados

    #Generar archivo txt que guarde los datos del sql para
    sql.to_csv('Universidad Tecnológica Nacional')

solucion_universidad_tres_de_febrero()
solucion_universidad_tecnologica_nacional()

  