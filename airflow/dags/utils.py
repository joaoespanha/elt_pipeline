def get_requirements_list():
    with open("/opt/airflow/elt/requirements.txt", "r") as file:
        requirements = file.read().splitlines()
        print(requirements)
    return requirements
