import logging
import os

import airflow
import pendulum
from airflow.decorators import task
from airflow.operators import bash, empty
from airflow.operators.python import get_current_context
from dags.common import slack

logger = logging.getLogger(__name__)

default_args = {
    "cwd": os.getenv("AIRFLOW_BASE_DIR"),
    "on_failure_callback": slack.task_fail_alert,
}


with airflow.DAG(
    dag_id="final_tables",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    @task(task_id="end")
    def end(**kwargs):
        slack.task_success_alert(get_current_context())

    dbt_debug = bash.BashOperator(
        task_id="dbt_debug",
        bash_command=f"dbt debug",
    )

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed",
    )

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run",
    )

    dbt_clean = bash.BashOperator(
        task_id="dbt_clean",
        bash_command=f"dbt clean",
    )

    end_task = end()

    (start >> dbt_debug >> dbt_seed >> dbt_run >> dbt_clean >> end_task)
