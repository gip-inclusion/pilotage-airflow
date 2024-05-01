import airflow
from airflow.operators import bash, empty

from dags.common import db, dbt, default_dag_args


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="generate_docs",
    schedule_interval="@daily",
    **dag_args,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    env_vars = db.connection_envvars()

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        env=env_vars,
        append_env=True,
    )

    dbt_generate_docs = bash.BashOperator(
        task_id="dbt_generate_docs",
        bash_command="rm -rf /tmp/dbt-docs && DBT_TARGET_PATH=/tmp/dbt-docs dbt docs generate",
        env=env_vars,
        append_env=True,
    )

    # these can stay as env vars since they are considered deployment secrets, not business vars.
    cellar_sync_cmd_parts = [
        "s3cmd",
        "--access_key=${S3_DOCS_ACCESS_KEY}",
        "--secret_key=${S3_DOCS_SECRET_KEY}",
        "sync",
        "--delete-removed",
        "--guess-mime-type",
        "--acl-public",
        "/tmp/dbt-docs/",
        "s3://${S3_DOCS_BUCKET}/",
    ]
    cellar_sync = bash.BashOperator(
        task_id="cellar_sync",
        bash_command=" ".join(cellar_sync_cmd_parts),
    )

    end = empty.EmptyOperator(task_id="end")

    (start >> dbt_generate_docs >> cellar_sync >> end)
