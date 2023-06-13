from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import dates, db, default_dag_args, slack


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

        df_int = []

        for name, pub_sheet_url in Variable.get("NPS_DASHBOARD_PILOTAGE", deserialize_json=True):
            print(f"reading {name=} at {pub_sheet_url=}")
            sheet_df = pd.read_csv(pub_sheet_url)
            sheet_df["Submitted at"] = pd.to_datetime(sheet_df["Submitted at"], dayfirst=True)
            sheet_df.rename(
                columns={
                    "Submitted at": "Date",
                    "Quelle est la probabilité que vous recommandiez ce tableau de bord à un collègue, "
                    "partenaire ou homologue ?": "Recommendation",
                },
                inplace=True,
            )
            sheet_df["Nom Du Tb"] = name
            sheet_df = sheet_df[["Recommendation", "Nom Du Tb", "Date"]]
            df_int.append(sheet_df)
        df = pd.concat(df_int)

        # Since this df is appended to a table where these columns exist they need to be created and added to the df
        columns = ["Utilité Indicateurs", "Prise De Decision Grace Au Tb", "Satisfaction Globale"]
        for col in columns:
            df[col] = np.nan
        start_of_previous_week, end_of_previous_week = dates.get_previous_week_range()
        df = df[(df["Date"] >= start_of_previous_week) & (df["Date"] <= end_of_previous_week)]

        df.to_sql("suivi_satisfaction", con=db.connection_engine(), if_exists="append", index=False)

    create_nps_task = create_nps()

    (start >> create_nps_task >> end)
