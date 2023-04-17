import airflow
import pendulum
from airflow.operators import bash, empty
from dags.common import dbt, default_dag_args, slack

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="generate_docs",
    schedule_interval="@daily",
    **dag_args,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    dbt_generate_docs = bash.BashOperator(
        task_id="dbt_generate_docs",
        bash_command="rm -rf /tmp/dbt-docs && DBT_TARGET_PATH=/tmp/dbt-docs dbt docs generate",
    )

    # FIXME(vperron): those environment variables should absolutely be versioned in itou-secrets.
    cellar_sync = bash.BashOperator(
        task_id="cellar_sync",
        bash_command="s3cmd --access_key=${S3_DOCS_ACCESS_KEY} --secret_key=${S3_DOCS_SECRET_KEY} sync --acl-public /tmp/dbt-docs s3://${S3_DOCS_BUCKET}",
    )

    end = slack.success_notifying_task()

    (start >> dbt_generate_docs >> cellar_sync >> end)
