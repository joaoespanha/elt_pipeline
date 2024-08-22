from utils import get_requirements_list

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


def run_elt_script():
    import subprocess

    script_path = "/opt/airflow/elt/main.py"
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script failed with error: {result.stderr}")
    else:
        print(result.stdout)


dag = DAG(
    "elt_pipeline",
    default_args=default_args,
    description="An ELT workflow",
    start_date=datetime(2023, 10, 3),
    catchup=False,
)

task_1 = PythonVirtualenvOperator(
    task_id="run_elt_script",
    requirements=get_requirements_list(),
    python_callable=run_elt_script,
    dag=dag,
)

task_1
