from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, Param

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG(
    dag_id="dbt_manual",
    schedule=None,
    params={
        "dbt_run_args": Param(
            type="string", description="write dbt run arguments, for example : --select stg_reseaux+"
        ),
    },
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command="dbt deps && dbt run {{ params.dbt_run_args }}",  # dbt deps is needed
        env=env_vars,
        append_env=True,
    )

    (dbt_run >> slack.success_notifying_task())
