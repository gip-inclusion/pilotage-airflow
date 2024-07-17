import sqlalchemy
from airflow import DAG
from airflow.decorators import task
from airflow.operators import empty

from dags.common import airtable, db, default_dag_args, slack


DB_SCHEMA = "monrecap"

with DAG(
    "mon_recap",
    schedule_interval="@daily",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="monrecap_airtable")
    def monrecap_airtable(**kwargs):
        import pandas as pd

        tables = ["Commandes", "Contacts", "Baromètre - Airflow"]
        for table_name in tables:
            url, headers = airtable.connection_airtable(table_name)
            df = airtable.fetch_airtable_data(url, headers)
            if table_name == "Commandes":
                df["Submitted at"] = pd.to_datetime(df["Submitted at"])
            if table_name == "Contacts":
                df["Date de première commande"] = pd.to_datetime(df["Date de première commande"])
                df["Date de dernière commande"] = pd.to_datetime(df["Date de dernière commande"])
            elif table_name == "Baromètre - Airflow":
                df["Submitted at"] = pd.to_datetime(df["Submitted at"])
                table_name = "barometre"

            df.to_sql(
                table_name,
                con=db.connection_engine(),
                schema=DB_SCHEMA,
                if_exists="replace",
                index=False,
                dtype={
                    "Validation manuelle": sqlalchemy.types.JSON,
                    "Demande de prise de RDV": sqlalchemy.types.JSON,
                },
            )

    monrecap_airtable = monrecap_airtable()

    (start >> monrecap_airtable >> end)
