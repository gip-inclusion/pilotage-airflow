from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty
from sqlalchemy.types import DateTime, Integer

from dags.common import db, default_dag_args, matomo, slack


with DAG(
    "suivi_visites_campagnes_c0",
    schedule_interval="@monthly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="get_visits_per_campaign")
    def get_visits_per_campaign(**kwargs):
        out_dtf = matomo.get_visits_per_campaign_from_matomo(
            Variable.get("MATOMO_BASE_URL"),
            Variable.get("GIP_MATOMO_TOKEN"),
        )
        out_dtf.to_sql(
            "visits_per_campaign_c0",
            con=db.connection_engine(),
            if_exists="replace",
            index=False,
            dtype={"date": DateTime, "duree": Integer},
        )

    visits_per_campaign = get_visits_per_campaign()

    (start >> visits_per_campaign >> end)
