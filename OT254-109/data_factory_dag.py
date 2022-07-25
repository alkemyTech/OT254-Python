from dag_factory import DAGFactory
from procesar_datos import procesar_unv_del_salvador , procesar_unv_Comahue

tasks = {}
tasks[procesar_unv_del_salvador] = []
tasks[procesar_unv_Comahue] = [] 
    
DAG_NAME = 'data_factory_dag' 
    
dag = DAGFactory().get_airflow_dag(DAG_NAME, tasks, cron='@daily')