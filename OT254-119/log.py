# Then use the config file in the code
import logging
import logging.config
import threading
from time import sleep

logging.config.fileConfig('/home/andresgb/airflow/dags/log.cfg')

def contar():
    '''Ejecutar dos funciones en paralelo'''
    logging.info('Iniciando contador')
    for i in range(1, 5):
        logging.info('Contando: {}'.format(i))
        sleep(2)
    logging.info('Fin del contador')

hilo1 = threading.Thread(target=contar)
hilo2 = threading.Thread(target=contar)
hilo1.start()
hilo2.start()

