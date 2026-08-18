import airflow
from airflow.operators import bash

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="dbt_imer",
    schedule="@daily",
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

    dbt_build = bash.BashOperator(
        task_id="dbt_build",
        # `cautious` évite de lancer les tests dont tous les modèles parents ne sont pas
        # dans la sélection (ex: le test equal_rowcount de stg_structures, qui référence
        # `structures` sans que stg_structures soit construit ici).
        bash_command='dbt build --select "+tag:imer" --indirect-selection cautious',
        env=env_vars,
        append_env=True,
    )

    (dbt_debug >> dbt_deps >> dbt_build >> slack.success_notifying_task())
