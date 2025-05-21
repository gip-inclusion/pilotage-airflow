import logging

from airflow import DAG
from dags.common import dbt, db, default_dag_args
from airflow.decorators import task
from airflow.operators import python

from dags.common.immersion_facilitee.helpers import get_all_items_last_5_years, get_dataframe_from_response, insert_data_to_db
from dags.common.immersion_facilitee.models import create_tables

logger = logging.getLogger(__name__)


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG("immersion_facilitee", schedule_interval="@weekly", **dag_args) as dag:

    @task(task_id="import_conventions")
    def import_conventions(**kwargs):
        response = get_all_items_last_5_years("/v2/conventions")
        df = get_dataframe_from_response(response)
        insert_data_to_db(df)
        logger.info("Import complete.")

    (
        python.PythonOperator(task_id="create_schema", python_callable=create_tables)
        >> [import_conventions()]
    )