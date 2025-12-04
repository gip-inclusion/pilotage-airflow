import logging

from airflow import DAG
from airflow.decorators import task

from dags.common import dbt, default_dag_args
from dags.common.tasks import create_models
from dags.common.to_dora.helpers import get_les_emplois_users, insert_data_to_dora_db
from dags.common.to_dora.models import RawLesEmploisBase


logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


with DAG("emplois_users_to_dora", schedule="@weekly", **dag_args) as dag:

    @task
    def export_users(**kwargs):
        les_emplois_users = get_les_emplois_users()

        insert_data_to_dora_db(les_emplois_users)

        logger.info("Exported %d Les-Emplois users to Dora.", len(les_emplois_users))

    create_models(RawLesEmploisBase).as_setup() >> export_users()
