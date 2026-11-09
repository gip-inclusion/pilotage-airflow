from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, CronTriggerTimetable, Param

from dags.common import db, dbt, default_dag_args, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

SEED_COMMAND = """
set -e
args=()
if [[ "$DBT_SEED_FULL_REFRESH" == 1 && -z "$DBT_SEED_NAME" ]]; then
    echo "full_refresh requires a seed_name." >&2
    exit 1
fi
if [[ -n "$DBT_SEED_NAME" ]]; then
    selected=$(dbt --quiet ls --resource-type seed --select "$DBT_SEED_NAME" --output name)
    if [[ "$selected" != "$DBT_SEED_NAME" ]]; then
        echo "seed_name must match exactly one existing seed name." >&2
        exit 1
    fi
    args+=(--select "$DBT_SEED_NAME")
fi
if [[ "$DBT_SEED_FULL_REFRESH" == 1 ]]; then
    args+=(--full-refresh)
fi
dbt seed "${args[@]}"
"""

with DAG(
    dag_id="dbt_prepare",
    schedule=CronTriggerTimetable("30 22 * * *", timezone="Europe/Paris"),
    params={
        "seed_name": Param(
            None,
            type=["null", "string"],
            pattern=r"^[A-Za-z0-9_]+$",
            description="Nom exact du seed à charger. Laisser vide pour charger tous les seeds.",
        ),
        "full_refresh": Param(False, type="boolean", description="Reconstruit le seed sélectionné."),
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

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command=SEED_COMMAND,
        env=env_vars
        | {
            "DBT_SEED_NAME": "{{ params.seed_name or '' }}",
            "DBT_SEED_FULL_REFRESH": "{{ 1 if params.full_refresh else 0 }}",
        },
        append_env=True,
    )

    dbt_debug >> dbt_seed >> slack.success_notifying_task()
