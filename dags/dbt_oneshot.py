import airflow
from airflow.operators import bash, empty, trigger_dagrun

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="dbt_oneshot",
    schedule=None,
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

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        env=env_vars,
        append_env=True,
    )

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed --full-refresh",
        env=env_vars,
        append_env=True,
    )

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --select staging marts.oneshot",
        env=env_vars,
        append_env=True,
    )

    trigger_data_consistency = trigger_dagrun.TriggerDagRunOperator(
        trigger_dag_id="data_consistency", task_id="trigger_data_consistency"
    )

    (start >> dbt_debug >> dbt_deps >> dbt_seed >> dbt_run >> trigger_data_consistency >> end)
