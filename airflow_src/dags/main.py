import logging

import airflow
import pendulum
from airflow.operators import bash, empty

logger = logging.getLogger(__name__)


with airflow.DAG(
    dag_id="main",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    dbt_debug = bash.BashOperator(
        task_id="dbt_debug",
        bash_command=f"dbt debug",
        cwd="/opt/airflow",
    )

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run",
        cwd="/opt/airflow",
    )

    dbt_clean = bash.BashOperator(
        task_id="dbt_clean",
        bash_command=f"dbt clean",
        cwd="/opt/airflow",
    )

    (start >> dbt_debug >> dbt_run >> dbt_clean >> end)
