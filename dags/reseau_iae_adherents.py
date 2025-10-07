from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from dags.common import db, default_dag_args, slack


RESEAU_IAE_TABLE_CREATE_SQL = """
DROP TABLE IF EXISTS {table_name} CASCADE;
CREATE TABLE {table_name}(
    "SIRET" VARCHAR(32),
    "Réseau IAE" VARCHAR(64)
);
"""

with DAG(
    "reseau_iae_adherents",
    schedule="@monthly",
    **default_dag_args(),
) as dag:

    @task
    def store_gsheets(**kwargs):
        import pandas as pd

        sheet_url = Variable.get("RESEAU_IAE_ADHERENTS_PUB_SHEET_URL")
        print(f"reading reseaux IAE SIRETs at {sheet_url=}")

        df_rsx = pd.read_csv(sheet_url)
        # the coerce is here because sometimes we get bad SIRETS that are not only number
        df_rsx["SIRET"] = pd.to_numeric(df_rsx["SIRET"], errors="coerce").astype("Int64")
        db.pg_store("reseau_iae_adherents", df_rsx, RESEAU_IAE_TABLE_CREATE_SQL)

    store_gsheets() >> slack.success_notifying_task()
