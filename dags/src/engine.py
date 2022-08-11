from sqlalchemy import create_engine
from decouple import config
import logging
import logging.config

logging.config.fileConfig('/home/jmsiro/airflow/log.cfg',)
logger = logging.getLogger(__name__)

def eng_con():
    """
    Creates a connection with the database.
    """
    logger.debug("Creating connection with DB Server...")
    en = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(config('POSTGRES_USER'), config('POSTGRES_PASSWORD'), config('DATABASE_HOST'), config('POSTGRES_PORT'), config('DATABASE_NAME'))
    return create_engine(en)
    
def eng_dis(en):
    """
    Shotsdown a connection with the database.
    """
    logger.debug("Closing connection...")
    return en.dispose()