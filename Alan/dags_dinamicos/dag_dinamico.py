from airflow import DAG
import dagfactory
import logging.config
from pathlib import Path
import pathlib

path_um_yaml = Path.joinpath(pathlib.Path(__file__).parent,"file_UM.yaml")
path_unrc_yaml = Path.joinpath(pathlib.Path(__file__).parent,"file_UNRC.yaml")
path = Path.joinpath(pathlib.Path(__file__).parent,"logging_configs.cfg")

logging.config.fileConfig(path)
logging.getLogger('root')

um = dagfactory.DagFactory(path_um_yaml)
unrc = dagfactory.DagFactory(path_unrc_yaml)

um.clean_dags(globals())
unrc.clean_dags(globals())

#Se crean los dags
um.generate_dags(globals())
unrc.generate_dags(globals())
logging.info("Se generan dags correctamente")
