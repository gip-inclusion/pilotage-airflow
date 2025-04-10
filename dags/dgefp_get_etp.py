from io import StringIO

from airflow import DAG
from airflow.decorators import task
from airflow.operators import empty

from dags.common import db, default_dag_args, ftp, slack


with DAG(
    "dgefp_etp",
    schedule_interval=None,
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = slack.success_notifying_task()

    @task(task_id="store_etp")
    def store_etp(**kwargs):
        import ftputil
        import pandas as pd

        host, user, password = ftp.bucket_connection()
        FILE_PREFIX = "iae_v1_0_suivi_hebdomadaire_"

        with ftputil.FTPHost(host, user, password) as host_ft:
            etp_file = None
            for filename in host_ft.listdir("dgefp"):
                if filename.startswith(FILE_PREFIX) and filename.endswith(".csv"):
                    # Get file modification time and add to list
                    etp_file = filename

            if not etp_file:
                raise Exception("No matching files found in the FTP directory")

            # get the most recent file
            DGEFP_RAW_DATA = f"dgefp/{etp_file}"

            print(f"Loading most recent file: {etp_file}")

            # Need to do this because setting the encoding on the pd.read_csv did not work
            with host_ft.open(DGEFP_RAW_DATA, "rb") as etp_loaded_data:
                content = etp_loaded_data.read()
                text_content = content.decode("latin1")
                df = pd.read_csv(StringIO(text_content), sep=";")

                for col in df.columns[9:17]:
                    if df[col].isnull().any() or (df[col] == "").any():
                        print(f"Column {col} has missing or invalid values")
                        df[col] = df[col].replace("", "NaN")
                    else:
                        df[col] = df[col].str.replace(",", ".").astype(float)

        df.to_sql("dgefp_donnees_etp", con=db.connection_engine(), if_exists="replace", index=False)

    store_etp = store_etp()

    (start >> store_etp >> end)
