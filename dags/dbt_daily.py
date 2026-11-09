import pendulum
from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, Param, task

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {
    "default_args": dbt.get_default_args(),
    "start_date": pendulum.datetime(2026, 8, 26, tz="Europe/Paris"),
}

with DAG(
    dag_id="dbt_daily",
    schedule="0 3 * * *",
    params={
        "full_refresh": Param(False, type="boolean"),
    },
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    @task
    def params_check(params=None, **kwargs):
        is_full_refresh = params.get("full_refresh")
        if is_full_refresh:
            kwargs["ti"].xcom_push(
                "dbt_run_args", "--full-refresh --select staging +marts.daily+ +legacy.daily+ +marts.marts_core.daily+"
            )
        else:
            kwargs["ti"].xcom_push(
                "dbt_run_args", "--select staging +marts.daily+ +legacy.daily+ +marts.marts_core.daily+"
            )

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command="dbt run {{ ti.xcom_pull(task_ids='params_check', key='dbt_run_args') }}",
        env=env_vars,
        append_env=True,
    )

    dbt_test = bash.BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --select +marts.daily+ +legacy.daily+ +marts.marts_core.daily+",
        env=env_vars,
        append_env=True,
    )

    (params_check() >> dbt_run >> dbt_test >> slack.success_notifying_task())
