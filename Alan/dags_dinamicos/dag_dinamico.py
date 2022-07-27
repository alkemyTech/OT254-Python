from airflow import DAG
import dagfactory


um = dagfactory.DagFactory("/home/alan/airflow/dags/file_UM.yaml")
unrc = dagfactory.DagFactory("/home/alan/airflow/dags/file_UNRC.yaml")

um.clean_dags(globals())
unrc.clean_dags(globals())

#genera dags
um.generate_dags(globals())
unrc.generate_dags(globals())