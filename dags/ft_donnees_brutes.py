from airflow import DAG
from airflow.decorators import task

from dags.common import db, default_dag_args, ftp, slack


FT_RAW_DATA = "raw_data_ft.csv"

with DAG(
    "france_travail",
    schedule=None,
    **default_dag_args(),
) as dag:

    @task(task_id="store_ft")
    def store_ft(**kwargs):
        import ftputil
        import pandas as pd

        db.drop_view("stg_france_travail")

        with ftputil.FTPHost(*ftp.bucket_connection()) as host_ft:
            # Check if the remote file exists
            if host_ft.path.isfile(FT_RAW_DATA):
                # Download the file to a local temporary file
                with host_ft.open(FT_RAW_DATA, "rb") as ft_loaded_data:
                    ft_data = pd.read_csv(ft_loaded_data, sep="$")
                    freins_cols = [
                        "FREIN_ENTREE_FAMILLE",
                        "FREIN_ENTREE_ADMIN",
                        "FREIN_ENTREE_FINANCE",
                        "FREIN_ENTREE_LOGEMENT",
                        "FREIN_ENTREE_LANGUESAVOIR",
                        "FREIN_ENTREE_MOBILITE",
                        "FREIN_ENTREE_NUMERIQUE",
                        "FREIN_ENTREE_SANTE",
                        "FREIN_ENTREE_FORMATION",
                    ]
                    ft_data[freins_cols] = ft_data[freins_cols].replace("YYYY", None)
                    for col in freins_cols:
                        ft_data[col] = pd.to_numeric(ft_data[col])
            else:
                print(f"Remote file {FT_RAW_DATA} does not exist.")

        ft_data.to_sql("FT_donnees_brutes", con=db.connection_engine(), if_exists="replace", index=False)

    store_ft() >> slack.success_notifying_task()
