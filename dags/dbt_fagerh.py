import csv
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators import bash

from dags.common import db, dbt, default_dag_args


CSV_URL = (
    "https://grist.numerique.gouv.fr/o/gipinclusion/api/docs/"
    "adcjAbtXzq25AFYkHvdwgt/download/csv"
    "?viewSection=1&tableId=Reponses&activeSortSpec=%5B%5D&filters=%5B%5D"
    "&linkingFilter=%7B%22filters%22%3A%7B%7D%2C%22operations%22%3A%7B%7D%7D"
)
DAG_ID = "fagerh_csv_ingestion"
TEMP_FILENAME = "fagerh_reponses.csv"
LOCAL_TMP_ROOT = Path("/tmp") / DAG_ID
RAW_SCHEMA = "raw"
RAW_TABLE_NAME = "fagerh_reponses"

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    @task
    def download_csv():
        LOCAL_TMP_ROOT.mkdir(parents=True, exist_ok=True)
        output_path = LOCAL_TMP_ROOT / TEMP_FILENAME

        print(f"{CSV_URL=}")
        print(f"{output_path=}")

        response = requests.get(CSV_URL, timeout=60)
        response.raise_for_status()

        output_path.write_bytes(response.content)

        with output_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)

            header = next(reader, None)
            if not header:
                raise ValueError("error=csv_header_missing")

            first_row = next(reader, None)
            if not first_row:
                raise ValueError("error=csv_no_data_rows")

        print("status=success")

        return str(output_path)

    @task
    def load_raw_csv_to_db(file_path):
        db_table = f"{RAW_SCHEMA}.{RAW_TABLE_NAME}"

        print(f"{file_path=}")
        print(f"{db_table=}")

        df = pd.read_csv(file_path, dtype=str)

        with db.connection_engine().begin() as conn:
            df.to_sql(
                RAW_TABLE_NAME,
                con=conn,
                schema=RAW_SCHEMA,
                if_exists="replace",
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
        bash_command="dbt build --select stg_fagerh_reponses",
        env=env_vars,
        append_env=True,
    )

    downloaded = download_csv()
    loaded = load_raw_csv_to_db(downloaded)

    loaded >> dbt_debug >> dbt_deps >> dbt_build
