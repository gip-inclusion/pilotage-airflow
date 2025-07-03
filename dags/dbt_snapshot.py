import airflow
from airflow.models.param import Param
from airflow.operators import bash, trigger_dagrun

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="dbt_snapshot",
    schedule="@weekly",
    params={
        "full_refresh": Param(False, type="boolean"),
    },
    **dag_args,
) as dag:
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

    dbt_snapshot = bash.BashOperator(
        task_id="dbt_snapshot",
        bash_command="dbt snapshot",
        env=env_vars,
        append_env=True,
    )

    trigger_data_consistency = trigger_dagrun.TriggerDagRunOperator(
        trigger_dag_id="data_consistency", task_id="trigger_data_consistency"
    )

    dbt_debug >> dbt_deps >> dbt_snapshot >> trigger_data_consistency >> slack.success_notifying_task()
