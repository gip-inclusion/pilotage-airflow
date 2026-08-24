import pendulum
from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, Param, task

from dags.common import db, dbt, default_dag_args, slack
from dags.common.anonymize_sensitive_columns import DAILY_TABLES_TO_ANONYMIZE, anonymize_nir


dag_args = default_dag_args() | {
    "default_args": dbt.get_default_args(),
    "start_date": pendulum.datetime(2026, 8, 26, tz="Europe/Paris"),
}

with DAG(
    dag_id="dbt_daily",
    schedule="30 0 * * *",  # matches the end of the emplois update
    params={
        "full_refresh": Param(False, type="boolean"),
        "anonymize_nir": Param(True, type="boolean"),
    },
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    dbt_debug = bash.BashOperator(
        task_id="dbt_debug",
        bash_command="dbt debug",
        # keep going when the upstream anonymization task is skipped
        trigger_rule="none_failed",
        env=env_vars,
        append_env=True,
    )

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        env=env_vars,
        append_env=True,
    )

    @task
    def params_check(params=None, **kwargs):
        is_full_refresh = params.get("full_refresh")
        if is_full_refresh:
            kwargs["ti"].xcom_push("dbt_seed_args", "--full-refresh")
            kwargs["ti"].xcom_push(
                "dbt_run_args", "--full-refresh --select staging +marts.daily+ +legacy.daily+ +marts.marts_core.daily+"
            )
        else:
            kwargs["ti"].xcom_push("dbt_seed_args", "")
            kwargs["ti"].xcom_push(
                "dbt_run_args", "--select staging +marts.daily+ +legacy.daily+ +marts.marts_core.daily+"
            )

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed {{ ti.xcom_pull(task_ids='params_check', key='dbt_seed_args') }}",
        env=env_vars,
        append_env=True,
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

    (
        params_check()
        >> anonymize_nir(DAILY_TABLES_TO_ANONYMIZE)
        >> dbt_debug
        >> dbt_deps
        >> dbt_seed
        >> dbt_run
        >> dbt_test
        >> slack.success_notifying_task()
    )
