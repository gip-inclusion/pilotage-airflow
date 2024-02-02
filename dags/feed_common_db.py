import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import bash, empty

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

# TODO:
# - get env variables the Airflow way.
# - split this big task into smaller ones, ideally into flows with branches.
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
with airflow.DAG(
    dag_id="feed_common_db",
    **dag_args,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="generate_tables")
    def generate_tables(**kwargs):
        import os
        import tempfile
        from collections import namedtuple
        from ftplib import FTP
        from pathlib import Path

        import pandas as pd
        from sqlalchemy import create_engine

        Source = namedtuple("Source", ["table_name", "csv_name"])

        data_sources = [
            Source(table_name="if_conventions", csv_name="if.csv"),
            Source(table_name="dora", csv_name="dora_n.csv"), # sep should be a comma not a semicolon.
        ]

        engine = create_engine(os.getenv("COMMON_DB_PG_URI"))

        with tempfile.TemporaryDirectory() as tmpdir:
            with FTP(
                host=os.getenv("COMMON_DB_FTP_HOST"),
                user=os.getenv("COMMON_DB_FTP_USER"),
                passwd=os.getenv("COMMON_DB_FTP_PASSWORD")
            ) as ftp:
                for data_source in data_sources:
                    file_path = Path(tmpdir, data_source.csv_name)
                    with open(file_path, 'wb') as fp:
                        ftp.retrbinary(f'RETR {data_source.csv_name}', fp.write)
                    df = pd.read_csv(file_path)
                    df.to_sql(data_source.table_name, engine, if_exists='replace', index=False)



    generate_tables = generate_tables()

    (start >> generate_tables >> end)
