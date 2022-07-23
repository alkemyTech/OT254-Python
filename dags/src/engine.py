from sqlalchemy import create_engine
import logging
from decouple import config

logger = logging.getLogger(__name__)

def eng_con():
    """
    Creates a connection with the database.
    """
    # logging.info("Creating connection with DB Server...")
    en = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(config('POSTGRES_USER'), config('POSTGRES_PASSWORD'), config('DATABASE_HOST'), config('POSTGRES_PORT'), config('DATABASE_NAME'))
    return create_engine(en)
    
def eng_dis(en):
    """
    Shotsdown a connection with the database.
    """
    # logging.info("Closing connection...")
    return en.dispose()