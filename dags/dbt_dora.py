from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, CronTriggerTimetable

from dags.common import db, dbt, default_dag_args, slack


DAG_ID = "dbt_dora"

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


with DAG(
    dag_id=DAG_ID,
    schedule=CronTriggerTimetable("30 7 * * *", timezone="Europe/Paris"),
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    dbt_build = bash.BashOperator(
        task_id="dbt_build",
        bash_command='dbt build --select "+tag:dora" --exclude-resource-type seed --indirect-selection cautious',
        env=env_vars,
        append_env=True,
    )

    dbt_build >> slack.success_notifying_task()
