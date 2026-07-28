import io

import pandas as pd
import requests
import sqlalchemy
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import bash

from dags.common import db, dbt, default_dag_args


DAG_ID = "fagerh_csv_ingestion"
RAW_SCHEMA = "raw"
RAW_TABLE_NAME = "fagerh_reponses"
CSV_URL_VARIABLE_NAME = "FAGERH_SURVEY_URL"

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    @task
    def load_raw_csv_to_db():
        csv_url = Variable.get(CSV_URL_VARIABLE_NAME)
        db_table = f"{RAW_SCHEMA}.{RAW_TABLE_NAME}"

        print(f"{CSV_URL_VARIABLE_NAME=}")
        print(f"{db_table=}")

        response = requests.get(csv_url, timeout=60)
        response.raise_for_status()

        df = pd.read_csv(io.BytesIO(response.content), dtype=str)

        if df.empty:
            raise ValueError("error=csv_no_data_rows")

        with db.connection_engine().begin() as conn:
            conn.execute(sqlalchemy.text(f'DROP TABLE IF EXISTS "{RAW_SCHEMA}"."{RAW_TABLE_NAME}" CASCADE'))

            df.to_sql(
                RAW_TABLE_NAME,
                con=conn,
                schema=RAW_SCHEMA,
                index=False,
            )

        inserted_rows = len(df)

        print(f"{inserted_rows=}")
        print("status=loaded")

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
        bash_command='dbt build --select "+tag:fagerh"',
        env=env_vars,
        append_env=True,
    )

    loaded = load_raw_csv_to_db()

    loaded >> dbt_debug >> dbt_deps >> dbt_build
