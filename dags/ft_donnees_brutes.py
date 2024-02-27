from airflow import DAG
from airflow.decorators import task
from airflow.operators import empty

from dags.common import bucket, db, default_dag_args, slack


with DAG(
    "france_travail",
    schedule_interval="@weekly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="store_ft")
    def store_ft(**kwargs):
        import ftputil
        import pandas as pd

        hostbucket, userbucket, passwordbucket = bucket.bucket_connection()

        FT_RAW_DATA = "raw_data_ft.csv"

        db.drop_view("stg_france_travail")

        with ftputil.FTPHost(hostbucket, userbucket, passwordbucket) as host:
            # Check if the remote file exists
            if host.path.isfile(FT_RAW_DATA):
                # Download the file to a local temporary file
                with host.open(FT_RAW_DATA, "rb") as ft_loaded_data:
                    df = pd.read_csv(ft_loaded_data, sep="$")
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
                    df[freins_cols] = df[freins_cols].replace("YYYY", None)
                    for col in freins_cols:
                        df[col] = pd.to_numeric(df[col])
            else:
                print(f"Remote file {FT_RAW_DATA} does not exist.")
        df.to_sql("FT_donnees_brutes", con=db.connection_engine(), if_exists="replace", index=False)

    store_ft = store_ft()

    (start >> store_ft >> end)
