from airflow import DAG
from airflow.operators import bash

from dags.common import db, dbt, default_dag_args, slack


DAG_ID = "dbt_dora"

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        env=env_vars,
        append_env=True,
    )

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed",
        env=env_vars,
        append_env=True,
    )

    dbt_build = bash.BashOperator(
        task_id="dbt_build",
        bash_command='dbt build --select "+tag:dora"',
        env=env_vars,
        append_env=True,
    )

    dbt_deps >> dbt_seed >> dbt_build >> slack.success_notifying_task()
