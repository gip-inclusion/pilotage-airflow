from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import db, default_dag_args, slack


with DAG(
    "nps_pilotage",
    schedule_interval="@weekly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="create_nps")
    def create_nps(**kwargs):
        import pandas as pd

        df_int = []

        for name, pub_sheet_url in Variable.get("NPS_DASHBOARD_PILOTAGE", deserialize_json=True):
            print(f"reading {name=} at {pub_sheet_url=}")
            sheet_df = pd.read_csv(pub_sheet_url)
            sheet_df.rename(
                columns={
                    "Submitted at": "Date",
                    # Here the two questions are almost identicals because it's the tally question that changed.
                    # But the final output is the same, thus the two lines.
                    "Quelle est la probabilité que vous recommandiez ce tableau de bord à un collègue, "
                    "partenaire ou homologue ?": "Recommendation",
                    "Quelle est la probabilité que vous recommandiez ce tableau de bord à un partenaire "
                    "ou homologue ?": "Recommendation",
                },
                inplace=True,
            )
            sheet_df["Nom Du Tb"] = name
            sheet_df = sheet_df[["Recommendation", "Nom Du Tb", "Date"]]
            df_int.append(sheet_df)
        df = pd.concat(df_int)

        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)

        df.to_sql("suivi_satisfaction", con=db.connection_engine(), if_exists="replace", index=False)

    create_nps_task = create_nps()

    (start >> create_nps_task >> end)
