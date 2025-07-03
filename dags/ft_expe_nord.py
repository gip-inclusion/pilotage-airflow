from airflow import DAG
from airflow.decorators import task
from airflow.operators import empty

from dags.common import db, default_dag_args, ftp, slack


with DAG(
    "ft_expe_nord",
    schedule=None,
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="store_gsheets")
    def store_gsheets(**kwargs):
        import ftputil
        import pandas as pd

        with db.MetabaseDBCursor() as (_, conn):
            host, user, password = ftp.bucket_connection()
            ft_data = "ft_expe_nord.csv"

            with ftputil.FTPHost(host, user, password) as host_ft:
                # Check if the remote file exists
                if host_ft.path.isfile(ft_data):
                    # Download the file to a local temporary file
                    with host_ft.open(ft_data, "rb") as ft_loaded_data:
                        dtf = pd.read_csv(ft_loaded_data, index_col=None, sep=";")
                else:
                    print(f"Remote file {ft_data} does not exist.")
            dtf.to_sql("ft_iae_nord", con=db.connection_engine(), index=False, if_exists="replace")

    store_gsheet_task = store_gsheets()

    (start >> store_gsheet_task >> end)
