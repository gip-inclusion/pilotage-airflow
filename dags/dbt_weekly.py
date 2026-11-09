from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, Param, task

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG(
    dag_id="dbt_weekly",
    schedule=None,
    params={"full_refresh": Param(False, type="boolean")},
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    @task
    def params_check(params=None, **kwargs):
        is_full_refresh = params.get("full_refresh")
        if is_full_refresh:
            kwargs["ti"].xcom_push("dbt_run_args", "--full-refresh --exclude marts.daily marts.oneshot marts.manual")
        else:
            kwargs["ti"].xcom_push("dbt_run_args", "--select staging +marts.weekly+ +legacy.weekly+")

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command="dbt run {{ ti.xcom_pull(task_ids='params_check', key='dbt_run_args') }}",
        env=env_vars,
        append_env=True,
    )

    dbt_test = bash.BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --select +marts.weekly+ +legacy.weekly+",
        env=env_vars,
        append_env=True,
    )

    (params_check() >> dbt_run >> dbt_test >> slack.success_notifying_task())
