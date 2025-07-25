from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import dbt, default_dag_args, slack
from dags.common.monrecap.helpers import get_data_from_sheet, get_variables
from dags.common.monrecap.models import build_monrecap_baro_model, create_tables, insert_data_to_db


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG("monrecap_barometre", schedule_interval="5 0 * * *", **dag_args) as dag:

    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="create_table")
    def create_table(variables):
        create_tables(variables)

    @task(task_id="import_monrecap_baro")
    def import_monrecap_baro(variables):
        monrecap_baro_model = build_monrecap_baro_model(variables)
        data = get_data_from_sheet(Variable.get("BAROMETRE_MON_RECAP"), variables)
        insert_data_to_db(monrecap_baro_model, data)

    variables = get_variables()
    start >> create_table(variables) >> import_monrecap_baro(variables) >> end
