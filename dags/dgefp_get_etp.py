from io import StringIO

from airflow import DAG
from airflow.decorators import task
from airflow.operators import empty

from dags.common import db, default_dag_args, ftp, slack


with DAG(
    "dgefp_etp",
    schedule=None,
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = slack.success_notifying_task()

    @task(task_id="store_etp")
    def store_etp(**kwargs):
        import ftputil
        import pandas as pd

        with ftputil.FTPHost(*ftp.bucket_connection()) as host_ft:
            etp_file = None
            for filename in host_ft.listdir("dgefp"):
                if filename.startswith("iae_v1_0_suivi_hebdomadaire_") and filename.endswith(".csv"):
                    # Get file modification time and add to list
                    etp_file = filename

            if not etp_file:
                raise Exception("No matching files found in the FTP directory")

            # get the most recent file
            print(f"Loading most recent file: {etp_file}")

            # Need to do this because setting the encoding on the pd.read_csv did not work
            with host_ft.open(f"dgefp/{etp_file}", "rb") as etp_loaded_data:
                etp_data = pd.read_csv(StringIO(etp_loaded_data.read().decode("latin1")), sep=";")

                for col in etp_data.columns[9:17]:
                    if etp_data[col].isna().any() or (etp_data[col] == "").any():
                        print(f"Column {col} has missing or invalid values")
                        etp_data[col] = etp_data[col].replace("", "NaN")
                    else:
                        etp_data[col] = etp_data[col].str.replace(",", ".").astype(float)

        etp_data.to_sql("dgefp_donnees_etp", con=db.connection_engine(), if_exists="replace", index=False)

    store_etp = store_etp()

    (start >> store_etp >> end)
