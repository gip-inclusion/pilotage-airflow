import csv
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task

from dags.common import db, default_dag_args


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


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    **default_dag_args(),
) as dag:

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

        db.create_schema(RAW_SCHEMA)

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

    downloaded = download_csv()
    load_raw_csv_to_db(downloaded)
