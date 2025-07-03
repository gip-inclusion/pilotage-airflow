import airflow
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators import bash

from dags.common import db, dbt, default_dag_args


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="data_consistency",
    schedule=None,
    params={"all_tests": Param(False, type="boolean")},
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        env=env_vars,
        append_env=True,
    )

    @task
    def params_check(params=None, **kwargs):
        is_all_tests = params.get("all_tests")
        if is_all_tests:
            kwargs["ti"].xcom_push("dbt_test_args", "")
        else:
            kwargs["ti"].xcom_push("dbt_test_args", "--exclude test_etp_dgefp_pilo")

    dbt_test = bash.BashOperator(
        task_id="dbt_test",
        bash_command="dbt test {{ ti.xcom_pull(task_ids='params_check', key='dbt_test_args') }}",
        env=env_vars,
        append_env=True,
    )

    params_check() >> dbt_deps >> dbt_test
