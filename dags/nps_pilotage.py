from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import dates, default_dag_args, slack


with DAG(
    "nps_pilotage",
    schedule_interval="@weekly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="create_nps")
    def create_nps(**kwargs):
        import numpy as np
        import pandas as pd
        from sqlalchemy import create_engine

        df_int = []

        database = Variable.get("PGDATABASE")
        host = Variable.get("PGHOST")
        password = Variable.get("PGPASSWORD")
        port = Variable.get("PGPORT")
        user = Variable.get("PGUSER")

        for name, pub_sheet_url in Variable.get("NPS_DASHBOARD_PILOTAGE", deserialize_json=True):
            print(f"reading {name=} at {pub_sheet_url=}")
            sheet_df = pd.read_csv(pub_sheet_url)
            sheet_df["Submitted at"] = pd.to_datetime(sheet_df["Submitted at"], dayfirst=True)
            sheet_df.rename(
                columns={
                    "Submitted at": "Date",
                    "Quelle est la probabilité que vous recommandiez ce tableau de bord à un collègue, partenaire ou homologue ?": "Recommendation",
                },
                inplace=True,
            )
            sheet_df["Nom Du Tb"] = name
            sheet_df = sheet_df[["Date", "Recommendation", "Nom Du Tb"]]
            df_int.append(sheet_df)
        df = pd.concat(df_int)

        colonnes = ["Utilité Indicateurs", "Prise De Decision Grace Au Tb", "Satisfaction Globale"]
        for col in colonnes:
            df[col] = np.nan
        df = df[
            [
                "Utilité Indicateurs",
                "Prise De Decision Grace Au Tb",
                "Satisfaction Globale",
                "Recommendation",
                "Nom Du Tb",
                "Date",
            ]
        ]
        df = df[(df["Date"] >= dates.start_of_previous_week) & (df["Date"] <= dates.end_of_previous_week)]

        url = "postgresql://" + user + ":" + password + "@" + host + ":" + port + "/" + database
        engine = create_engine(url)
        df.to_sql("suivi_satisfaction", con=engine, if_exists="append", index=False)

    create_nps_task = create_nps()

    (start >> create_nps_task >> end)
