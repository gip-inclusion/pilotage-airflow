from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from dags.common import dbt, default_dag_args
from dags.common.esat.helpers import get_data_from_sheet, get_variables_dict, get_variables_types
from dags.common.esat.models import build_esat_model, create_tables, insert_data_to_db


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}
sheet_url = Variable.get("ESAT_SHEET_URL")


with DAG("esat", schedule_interval="@weekly", **dag_args) as dag:

    @task(task_id="create_table")
    def create_table(**kwargs):
        create_tables()

    @task(task_id="import_esat")
    def import_esat(**kwargs):
        variables_dict = get_variables_dict()
        variables_types = get_variables_types()
        esat_model = build_esat_model()
        data = get_data_from_sheet(sheet_url, variables_dict, variables_types)

        insert_data_to_db(esat_model, data)

    create_table() >> import_esat()
