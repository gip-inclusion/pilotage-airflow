from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import db, default_dag_args, slack


RESEAU_IAE_TABLE_CREATE_SQL = """
DROP TABLE IF EXISTS {table_name} CASCADE;
CREATE TABLE {table_name}(
    "SIRET" VARCHAR(16),
    "Réseau IAE" VARCHAR(64)
);
"""

with DAG(
    "reseau_iae_adherents",
    schedule_interval="@monthly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="store_gsheets")
    def store_gsheets(**kwargs):
        import pandas as pd

        sheet_url = Variable.get("RESEAU_IAE_ADHERENTS_PUB_SHEET_URL")
        print(f"reading reseaux IAE SIRETs at {sheet_url=}")
        db.pg_store("reseau_iae_adherents", pd.read_csv(sheet_url), RESEAU_IAE_TABLE_CREATE_SQL)

    store_gsheet_task = store_gsheets()

    (start >> store_gsheet_task >> end)
