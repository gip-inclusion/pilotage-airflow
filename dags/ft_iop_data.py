from io import BytesIO

from airflow import DAG
from airflow.decorators import task

from dags.common import db, default_dag_args, ftp, slack


with DAG(
    "france_travail_iop",
    schedule=None,
    **default_dag_args(),
) as dag:

    @task
    def get_iop(**kwargs):
        import ftputil
        import pandas as pd

        df_list = []

        folder_path = "ft_iop_poc"

        with ftputil.FTPHost(*ftp.bucket_connection()) as host_ft:
            if not host_ft.path.isdir(folder_path):
                raise ValueError(f"Folder not found: {folder_path}")

            iop_files = [f for f in host_ft.listdir(folder_path) if f.endswith(".xlsx")]
            for filename in sorted(iop_files):
                file_path = host_ft.path.join(folder_path, filename)

                with host_ft.open(file_path, "rb") as remote_file:
                    file_content = remote_file.read()

                df = pd.read_excel(BytesIO(file_content))
                df_list.append(df)

        # Concatenate all dataframes
        df = pd.concat(df_list, ignore_index=True)

        # Transform the values from these columns into columns
        minima_sociaux_dummies = df["Minima sociaux"].str.get_dummies(sep=", ")
        freins_dummies = df["Contrainte(s) personnelle(s)"].str.get_dummies(sep="\n")

        # Combine with the original dataframe
        df = pd.concat([df, minima_sociaux_dummies, freins_dummies], axis=1)

        # get safir code and structure name
        df[["code_safir", "structure_suivie"]] = df["Structure principale de suivi"].str.split(" - ", expand=True)

        df["Date mise à jour du diagnostic"] = pd.to_datetime(df["Date mise à jour du diagnostic"])

        # remove impact otherwise the column is too long and the df to sql fails
        df.columns = df.columns.str.replace(r"\s*\(Impact\s+(?:FAIBLE|FORT|MOYEN)\)", "", regex=True)

        df.to_sql(
            "raw_ft_iop_data", con=db.connection_engine(), schema="france_travail", if_exists="replace", index=True
        )

    get_iop() >> slack.success_notifying_task()
