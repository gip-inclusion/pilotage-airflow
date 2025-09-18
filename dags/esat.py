from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from sqlalchemy.ext.declarative import declarative_base

from dags.common import dbt, default_dag_args, gsheet
from dags.common.tasks import create_models


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

DB_SCHEMA = "esat"
variables_files = Path(__file__).parent / "common" / "esat" / "ESAT_variables_questionnaire2025.csv"

# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase
EsatBase = declarative_base()


with DAG("esat", schedule="@daily", **dag_args) as dag:
    variables = gsheet.get_variables(variables_files, delimiter_type=";")
    esat_model = gsheet.build_data_model(  # Needs to be build before `create_models(EsatBase)`
        variables,
        DB_SCHEMA,
        EsatBase,
        primary_key="submission_id",
        tablename="raw_questionnaire_2025",
        classname="EsatTallyAnswers",
    )

    @task
    def import_esat(model, data_spec):
        data = gsheet.get_data_from_sheet(Variable.get("ESAT_SHEET_URL"), data_spec)
        gsheet.insert_data_to_db(model, data)

    create_models(EsatBase).as_setup() >> import_esat(esat_model, variables)
