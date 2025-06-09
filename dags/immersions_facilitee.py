import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators import python

from dags.common import dbt, default_dag_args
from dags.common.immersion_facilitee.helpers import get_all_items, get_dataframe_from_response, insert_data_to_db
from dags.common.immersion_facilitee.models import create_tables


logger = logging.getLogger(__name__)


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG("immersion_facilitee", schedule_interval="@weekly", **dag_args) as dag:

    @task(task_id="import_conventions")
    def import_conventions(**kwargs):
        response = get_all_items("/v2/conventions")
        if not response:
            logger.info("No new data to import.")
            return
        df = get_dataframe_from_response(response)
        insert_data_to_db(df)
        logger.info("Import complete.")

    python.PythonOperator(task_id="create_schema", python_callable=create_tables) >> import_conventions()
