import functools
import os

from airflow.providers.standard.operators import bash
from airflow.sdk import DAG

from dags.common import db, default_dag_args, slack


def get_default_args():
    return {
        "cwd": os.getenv("AIRFLOW_BASE_DIR"),
        "on_failure_callback": slack.task_fail_alert,
    }


def tag_build_dag(dag_id, schedule, tag, build_args="", **dag_kwargs):
    """DAG standard : dbt debug > dbt deps > dbt build --select "+tag:<tag>" > notification Slack."""
    dag_args = default_dag_args() | {"default_args": get_default_args()} | dag_kwargs

    with DAG(dag_id=dag_id, schedule=schedule, **dag_args) as dag:
        task = functools.partial(bash.BashOperator, env=db.connection_envvars(), append_env=True)
        build_command = " ".join(filter(None, [f'dbt build --select "+tag:{tag}"', build_args]))
        (
            task(task_id="dbt_debug", bash_command="dbt debug")
            >> task(task_id="dbt_deps", bash_command="dbt deps")
            >> task(task_id="dbt_build", bash_command=build_command)
            >> slack.success_notifying_task()
        )

    return dag
