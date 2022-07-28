from jinja2 import Environment, FileSystemLoader
import yaml
import os

# Gets directory of the generator
file_dir =os.path.dirname(os.path.abspath(__file__))
# Creates template environment and a loader that looks up the templates in the folder (in this case the templates folder was setted up as the same of the generator)
env = Environment(loader=FileSystemLoader(file_dir))
# Loads the template
template = env.get_template("template.jinja2")

for filename in os.listdir(file_dir):
    if filename.endswith(".yaml"):
        with open(f"{file_dir}/{filename}", "r") as configfile:
            # Parses the .yaml file into a Python Object (dict)
            config = yaml.safe_load(configfile)
            print(config)
            with open(f"/home/jmsiro/airflow/dags/dy_dag_{config['dag_id']}.py", "w") as f:
                # Renders the template with the defined variables
                f.write(template.render(config))