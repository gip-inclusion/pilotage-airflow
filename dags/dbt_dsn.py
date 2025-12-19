import airflow
from airflow.operators import bash

from dags.common import db, dbt, default_dag_args


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="dbt_dsn",
    schedule=None,
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

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed --full-refresh",
        env=env_vars,
        append_env=True,
    )

    dbt_run = bash.BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run --select +path:dbt/models/marts/oneshot/sorties_en_emploi "
            "+dim_secteur_activite_naf "
            "+dim_insee_zones_emploi"
        ),
        env=env_vars,
        append_env=True,
    )

    dbt_debug >> dbt_deps >> dbt_seed >> dbt_run
