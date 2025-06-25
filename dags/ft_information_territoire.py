import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators import python

from dags.common import default_dag_args, slack
from dags.common.france_travail import api as ft_api_helpers, models


logger = logging.getLogger(__name__)


# NOTE: We recuperate the stats on a quarterly basis. Since we don't know when the stats will be updated API-side
# we cannot reliably schedule this DAG, so we run it regularly.
with DAG(**default_dag_args(), dag_id="ft_information_territoire", schedule_interval="@weekly") as dag:

    @task(task_id="registered_jobseeker_stats_by_territory")
    def registered_jobseeker_stats_by_territory(**kwargs):
        # Import data from the API for each territory targeted.
        access_token = ft_api_helpers.request_access_token(format_for_header=True)

        for territory in ft_api_helpers.list_territories(access_token):
            ft_api_helpers.get_stats_for_territory(access_token, territory)

        logger.info("Import complete.")

    (
        python.PythonOperator(task_id="create_schema_and_tables", python_callable=models.create_tables).as_setup()
        >> registered_jobseeker_stats_by_territory()
        >> slack.success_notifying_task()
    )
