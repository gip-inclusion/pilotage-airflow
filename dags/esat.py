from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from dags.common import dbt, default_dag_args
from dags.common.esat.helpers import get_data_from_sheet, get_variables
from dags.common.esat.models import build_esat_model, create_tables, insert_data_to_db


logger = LoggingMixin().log

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG("esat", schedule_interval="@weekly", **dag_args) as dag:

    @task(task_id="get_variables")
    def get_variables_task():
        variables_def = get_variables()
        variables_quest = {variables_def[variable]["question"]: variable for variable in variables_def}
        variables_types = {variable: variables_def[variable]["type"] for variable in variables_def}
        return {"questions": variables_quest, "types": variables_types}

    @task(task_id="create_table")
    def create_table(variables):
        create_tables(variables["types"])

    @task(task_id="import_esat")
    def import_esat(variables):
        esat_model = build_esat_model(variables["types"])
        data = get_data_from_sheet(Variable.get("ESAT_SHEET_URL"), variables["questions"])
        insert_data_to_db(esat_model, data)

    variables_dict = get_variables_task()
    create_table(variables_dict) >> import_esat(variables_dict)
