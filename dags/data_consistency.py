import airflow
from airflow.operators import bash, empty

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="data_consistency",
    schedule_interval="@hourly",
    **dag_args,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    env_vars = db.connection_envvars()

    dbt_test = bash.BashOperator(
        task_id="dbt_test",
        bash_command="dbt test",
        env=env_vars,
        append_env=True,
    )

    end = slack.success_notifying_task()

    (start >> dbt_test >> end)
