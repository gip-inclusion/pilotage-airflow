import logging

import httpx
import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import bash
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.dialects import postgresql

from dags.common import db, dbt, default_dag_args, slack
from dags.common.dates import to_date
from dags.common.tasks import create_schema


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


with DAG("data_inclusion", schedule="@daily", **dag_args) as dag:
    env_vars = db.connection_envvars()

    @task
    def drop_tables():
        con = db.connection_engine()
        con.execute("""drop table if exists data_inclusion.services_v1 cascade;
                       drop table if exists data_inclusion.structures_v1 cascade;""")

    @task
    def import_structures(**kwargs):
        structures = pd.DataFrame(get_all_items("/api/v1/structures"))
        structures["date_maj"] = structures["date_maj"].apply(to_date)
        structures.to_sql(
            "structures_v1",
            con=db.connection_engine(),
            schema=DB_SCHEMA,
            if_exists="replace",
            index=False,
            dtype={
                "reseaux_porteurs": postgresql.ARRAY(sqlalchemy.types.Text),
                "doublons": postgresql.ARRAY(sqlalchemy.types.JSON),
            },
        )
        logger.info("%r rows created", len(structures.index))

    @task
    def import_services(**kwargs):
        services = pd.DataFrame(get_all_items("/api/v1/services"))
        services["date_maj"] = services["date_maj"].apply(to_date)
        services.to_sql(
            "services_v1",
            con=db.connection_engine(),
            schema=DB_SCHEMA,
            if_exists="replace",
            index=False,
            dtype={
                "thematiques": postgresql.ARRAY(sqlalchemy.types.Text),
                "publics": postgresql.ARRAY(sqlalchemy.types.Text),
                "modes_accueil": postgresql.ARRAY(sqlalchemy.types.Text),
                "zone_eligibilite": postgresql.ARRAY(sqlalchemy.types.Text),
                "modes_mobilisation": postgresql.ARRAY(sqlalchemy.types.Text),
                "mobilisable_par": postgresql.ARRAY(sqlalchemy.types.Text),
            },
        )
        logger.info("%r rows created", len(services.index))

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        trigger_rule=TriggerRule.ALL_DONE,
        env=env_vars,
        append_env=True,
    )

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed",
        env=env_vars,
        append_env=True,
    )

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --select data_inclusion",
        env=env_vars,
        append_env=True,
    )

    (
        create_schema(DB_SCHEMA).as_setup()
        >> drop_tables()
        >> [import_structures(), import_services()]
        >> dbt_deps
        >> dbt_seed
        >> dbt_run
        >> slack.success_notifying_task()
    )
