import datetime
from dag_factory import DAGFactory
from script import solucion_universidad_tecnologica_nacional, solucion_universidad_tres_de_febrero

tasks = {}
tasks[solucion_universidad_tecnologica_nacional] = []
tasks[solucion_universidad_tres_de_febrero] = []

DAG_NAME = 'dag_with_factory'

dag = DAGFactory().get_airflow_dag(DAG_NAME, tasks,cron='@daily')