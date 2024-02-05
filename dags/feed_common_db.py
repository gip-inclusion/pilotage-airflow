import os
from collections import namedtuple
from ftplib import FTP
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import create_engine

from dags.common import dbt, default_dag_args, slack


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
Source = namedtuple("Source", ["table_name", "csv_name"])

data_sources = [
    # This could be replaced by an API call to their Metabase.
    Source(table_name="if_conventions", csv_name="if.csv"),
    # Separator should be a comma not a semicolon.
    Source(table_name="dora", csv_name="dora_n.csv"),
]

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


@dag(
    schedule=None,
    **dag_args,
)
def feed_common_db():
    @task()
    def download_csv(csv_name):
        file_path = Path(AIRFLOW_HOME, csv_name)
        with FTP(
            host=Variable.get("COMMON_DB_FTP_HOST"),
            user=Variable.get("COMMON_DB_FTP_USER"),
            passwd=Variable.get("COMMON_DB_FTP_PASSWORD"),
        ) as ftp:
            # AIRFLOW_HOME is the only directory Airflow has writing rights.
            # Even subdirectories are read-only.
            with open(file_path, "wb") as fp:
                ftp.retrbinary(f"RETR {csv_name}", fp.write)

    @task()
    def populate_table(csv_name, table_name):
        file_path = Path(AIRFLOW_HOME, csv_name)
        engine = create_engine(Variable.get("COMMON_DB_PG_URI"))
        with open(file_path, "r") as fp:
            df = pd.read_csv(fp)
            return df.to_sql(table_name, engine, if_exists="replace", index=False)

    @task()
    def rm_file(csv_name):
        Path(AIRFLOW_HOME, csv_name).unlink()

    # Using the following syntax, which I find more readable, was causing an error because of an infinite loop in XCom:
    # python.PythonOperator(
    #     task_id=f"rm_file_{source.csv_name}",
    #     params={"csv_name": source.csv_name},
    #     python_callable=rm_file,
    processes = [
        download_csv.override(task_id=f"download_csv_{source.csv_name}")(csv_name=source.csv_name)
        >> populate_table.override(task_id=f"populate_table_{source.table_name}")(
            csv_name=source.csv_name, table_name=source.table_name
        )
        >> rm_file.override(task_id=f"rm_file_{source.csv_name}")(csv_name=source.csv_name)
        for source in data_sources
    ]

    end = slack.success_notifying_task()
    (processes >> end)


feed_common_db()
