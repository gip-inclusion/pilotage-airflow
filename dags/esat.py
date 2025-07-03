from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from dags.common import dbt, default_dag_args
from dags.common.esat.helpers import get_data_from_sheet, get_variables
from dags.common.esat.models import EsatBase, build_esat_model, insert_data_to_db
from dags.common.tasks import create_models


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG("esat", schedule="@daily", **dag_args) as dag:

    @task
    def import_esat(variables):
        esat_model = build_esat_model(variables)
        data = get_data_from_sheet(Variable.get("ESAT_SHEET_URL"), variables)
        insert_data_to_db(esat_model, data)

    variables = get_variables()
    esat_model = build_esat_model(variables)  # Needs to be build before `create_models(EsatBase)`

    create_models(EsatBase).as_setup() >> import_esat(variables)
