import airflow
from airflow.operators import bash, empty

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="final_tables",
    schedule_interval=None,
    **dag_args,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    env_vars = db.connection_envvars()

    dbt_debug = bash.BashOperator(
        task_id="dbt_debug",
        bash_command="dbt debug",
        env=env_vars,
        append_env=True,
    )

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed",
        env=env_vars,
        append_env=True,
    )

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command="dbt run",
        env=env_vars,
        append_env=True,
    )

    dbt_clean = bash.BashOperator(
        task_id="dbt_clean",
        bash_command="dbt clean",
        env=env_vars,
        append_env=True,
    )

    (start >> dbt_debug >> dbt_seed >> dbt_run >> dbt_clean >> end)