from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from dags.common import dbt, default_dag_args
from dags.common.esat.helpers import get_data_from_sheet, get_variables
from dags.common.esat.models import build_esat_model, create_tables, insert_data_to_db


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG("esat", schedule="@daily", **dag_args) as dag:

    @task(task_id="create_table")
    def create_table(variables):
        create_tables(variables)

    @task(task_id="import_esat")
    def import_esat(variables):
        esat_model = build_esat_model(variables)
        data = get_data_from_sheet(Variable.get("ESAT_SHEET_URL"), variables)
        insert_data_to_db(esat_model, data)

    variables = get_variables()
    create_table(variables) >> import_esat(variables)
