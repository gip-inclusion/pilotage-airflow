import logging

import httpx
import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import python
from sqlalchemy.dialects import postgresql

from dags.common import db, dbt, default_dag_args, slack


DB_SCHEMA = "data_inclusion"

logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


def api_client() -> httpx.Client:
    return httpx.Client(
        base_url=Variable.get("API_DATA_INCLUSION_BASE_URL"),
        headers={"Authorization": "Bearer {}".format(Variable.get("API_DATA_INCLUSION_TOKEN"))},
        timeout=httpx.Timeout(timeout=5, read=30),
    )


def get_all_items(path):
    client = api_client()

    page_to_fetch = 1
    while True:
        response = client.get(path, params={"size": 1000, "page": page_to_fetch})
        response.raise_for_status()
        data = response.json()
        logger.info("Got %r items, metadata=%r", len(data["items"]), {k: v for k, v in data.items() if k != "items"})
        yield from data["items"]
        page_to_fetch = data["page"] + 1
        if page_to_fetch > data["pages"]:
            break


with DAG("data_inclusion", schedule_interval="@daily", **dag_args) as dag:

    @task(task_id="import_structures")
    def import_structures(**kwargs):
        df = pd.DataFrame(get_all_items("/api/v0/structures"))
        df["date_maj"] = pd.to_datetime(df["date_maj"])
        row_created = df.to_sql(
            "structures_v0",
            con=db.connection_engine(),
            schema=DB_SCHEMA,
            if_exists="replace",
            index=False,
            dtype={
                "labels_nationaux": postgresql.ARRAY(sqlalchemy.types.Text),
                "labels_autres": postgresql.ARRAY(sqlalchemy.types.Text),
                "thematiques": postgresql.ARRAY(sqlalchemy.types.Text),
            },
        )
        logger.info("%r rows created", row_created)

    @task(task_id="import_services")
    def import_services(**kwargs):
        df = pd.DataFrame(get_all_items("/api/v0/services"))
        df["date_creation"] = pd.to_datetime(df["date_creation"])
        df["date_suspension"] = pd.to_datetime(df["date_suspension"])
        df["date_maj"] = pd.to_datetime(df["date_maj"])
        rows_created = df.to_sql(
            "services_v0",
            con=db.connection_engine(),
            schema=DB_SCHEMA,
            if_exists="replace",
            index=False,
            dtype={
                "types": postgresql.ARRAY(sqlalchemy.types.Text),
                "thematiques": postgresql.ARRAY(sqlalchemy.types.Text),
                "frais": postgresql.ARRAY(sqlalchemy.types.Text),
                "profils": postgresql.ARRAY(sqlalchemy.types.Text),
                "pre_requis": postgresql.ARRAY(sqlalchemy.types.Text),
                "justificatifs": postgresql.ARRAY(sqlalchemy.types.Text),
                "modes_accueil": postgresql.ARRAY(sqlalchemy.types.Text),
                "modes_orientation_beneficiaire": postgresql.ARRAY(sqlalchemy.types.Text),
                "modes_orientation_accompagnateur": postgresql.ARRAY(sqlalchemy.types.Text),
            },
        )
        logger.info("%r rows created", rows_created)

    (
        python.PythonOperator(task_id="create_schema", python_callable=db.create_schema, op_args=[DB_SCHEMA])
        >> [import_structures(), import_services()]
        >> slack.success_notifying_task()
    )
