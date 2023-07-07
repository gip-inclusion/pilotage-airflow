from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import db, default_dag_args, matomo, slack


with DAG(
    "suivi_visites_campagnes_c0",
    schedule_interval="@weekly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="get_visits_per_campaign")
    def get_visits_per_campaign(**kwargs):
        """
        extract data from matomo and update the table visits_per_campaign_c0
        """
        tok = Variable.get("TOKEN_MATOMO_GIP")
        out_dtf = matomo.get_visits_per_campaign(tok)
        out_dtf.to_sql("visits_per_campaign_c0", con=db.connection_engine(), if_exists="replace", index=False)
        print("saved")
        print(out_dtf)

    visits_per_campaign = get_visits_per_campaign()

    (start >> visits_per_campaign >> end)
