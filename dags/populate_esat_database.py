import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators import bash

from dags.common import db, dbt, default_dag_args, slack


SOURCE_SCHEMA = "public"
SOURCE_TABLE = "surveys_esatanswer"

TARGET_SCHEMA = "raw"
TARGET_TABLE = "surveys_esatanswer"

logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


with DAG("populate_esat_database", schedule="@daily", **dag_args) as dag:
    env_vars = db.connection_envvars()

    @task
    def export_esat_table():
        query = f'SELECT * FROM "{SOURCE_SCHEMA}"."{SOURCE_TABLE}";'

        with (
            db.DBConnection(db_url_variable="PILO_FRONT_DB_URL_SECRET") as src_db,
            db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET") as dst_db,
        ):
            first_write = True

            for chunk in src_db.query_chunked(query):
                dst_db.to_sql(
                    chunk,
                    table=TARGET_TABLE,
                    schema=TARGET_SCHEMA,
                    if_exists="replace" if first_write else "append",
                )

                logger.info(
                    "Exported %d rows to %s.%s",
                    len(chunk),
                    TARGET_SCHEMA,
                    TARGET_TABLE,
                )
                first_write = False

    dbt_debug = bash.BashOperator(
        task_id="dbt_debug",
        bash_command="dbt debug",
        env=env_vars,
        append_env=True,
    )

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        env=env_vars,
        append_env=True,
    )

    dbt_build = bash.BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --select +path:dbt/models/marts/daily/esat",
        env=env_vars,
        append_env=True,
    )

    export_esat_table() >> dbt_debug >> dbt_deps >> dbt_build >> slack.success_notifying_task()
