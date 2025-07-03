import logging

from airflow import DAG
from airflow.decorators import task

from dags.common import dbt, default_dag_args
from dags.common.immersion_facilitee.helpers import get_all_items, get_dataframe_from_response, insert_data_to_db
from dags.common.immersion_facilitee.models import ImmersionFaciliteeBase
from dags.common.tasks import create_models


logger = logging.getLogger(__name__)


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG("immersion_facilitee", schedule="@weekly", **dag_args) as dag:

    @task
    def import_conventions(**kwargs):
        response = get_all_items("/v2/conventions")
        if not response:
            logger.info("No new data to import.")
            return
        insert_data_to_db(get_dataframe_from_response(response))
        logger.info("Import complete.")

    create_models(ImmersionFaciliteeBase).as_setup() >> import_conventions()
