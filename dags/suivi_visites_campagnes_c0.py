from airflow.sdk import DAG, CronTriggerTimetable, Variable, task
from sqlalchemy.types import DateTime, Integer

from dags.common import db, default_dag_args, matomo, slack


with DAG(
    "suivi_visites_campagnes_c0",
    schedule=CronTriggerTimetable("15 12 1 * *", timezone="Europe/Paris"),
    **default_dag_args(),
) as dag:

    @task
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

    get_visits_per_campaign() >> slack.success_notifying_task()
