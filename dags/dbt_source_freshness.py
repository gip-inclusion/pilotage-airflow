import pendulum
from airflow.providers.standard.operators import bash
from airflow.sdk import DAG

from dags.common import db, dbt, default_dag_args


dag_args = default_dag_args() | {
    "default_args": dbt.get_default_args(),
    "start_date": pendulum.datetime(2026, 10, 29, tz="Europe/Paris"),
}

with DAG(dag_id="dbt_source_freshness", schedule="0 17 * * *", **dag_args) as dag:
    bash.BashOperator(
        task_id="dbt_source_freshness",
        bash_command="dbt source freshness",
        env=db.connection_envvars(),
        append_env=True,
    )
