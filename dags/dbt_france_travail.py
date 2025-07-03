import airflow
from airflow.models.param import Param
from airflow.operators import bash, python

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="dbt_france_travail",
    schedule=None,
    params={
        "donnees_prod": Param(False, type="boolean"),
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

    def params_check(params=None, **kwargs):
        is_prod = params.get("donnees_prod")
        if is_prod:
            kwargs["ti"].xcom_push("dbt_seed_args", "--full-refresh")
            kwargs["ti"].xcom_push("dbt_run_args", "--select stg_france_travail france_travail_donnees_prod")
        else:
            kwargs["ti"].xcom_push("dbt_seed_args", "--full-refresh")
            kwargs["ti"].xcom_push("dbt_run_args", "--select stg_france_travail france_travail_donnees_recette")

    params_check = python.PythonOperator(task_id="params_check", python_callable=params_check)

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

    dbt_debug >> dbt_deps >> dbt_seed >> dbt_run >> slack.success_notifying_task()
