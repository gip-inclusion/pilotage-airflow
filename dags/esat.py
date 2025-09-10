from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from dags.common import dbt, default_dag_args, gsheet
from dags.common.esat.models import DB_SCHEMA, EsatBase, create_tables


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

VARIABLES_FILE = Path("/opt/airflow/dags/common/esat/ESAT_variables_questionnaire2025.csv")
delimiter_type = ";"
primary_key = "submission_id"
tablename = "questionnaire_2025"
classname = "EsatTallyAnswers"

with DAG("esat", schedule_interval="@weekly", **dag_args) as dag:

    @task(task_id="create_table")
    def create_table(variables):
        create_tables(variables)

    @task(task_id="import_esat")
    def import_esat(variables):
        esat_model = gsheet.build_data_model(variables, primary_key, tablename, DB_SCHEMA, classname, EsatBase)
        data = gsheet.get_data_from_sheet(Variable.get("ESAT_SHEET_URL"), variables)
        gsheet.insert_data_to_db(esat_model, data)

    variables = gsheet.get_variables(VARIABLES_FILE, delimiter_type)
    create_table(variables) >> import_esat(variables)
