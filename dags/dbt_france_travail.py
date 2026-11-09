from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, Param, task

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG(
    dag_id="dbt_france_travail",
    schedule=None,
    params={
        "donnees_prod": Param(False, type="boolean"),
    },
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    @task
    def params_check(params=None, **kwargs):
        is_prod = params.get("donnees_prod")
        if is_prod:
            kwargs["ti"].xcom_push("dbt_run_args", "--select stg_france_travail france_travail_donnees_prod")
        else:
            kwargs["ti"].xcom_push("dbt_run_args", "--select stg_france_travail france_travail_donnees_recette")

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command="dbt run {{ ti.xcom_pull(task_ids='params_check', key='dbt_run_args') }}",
        env=env_vars,
        append_env=True,
    )

    params_check() >> dbt_run >> slack.success_notifying_task()
