import os

import airflow
from airflow.models import Variable
from airflow.operators import bash
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

from dags.common import db, dbt, default_dag_args
from dags.common.operators import S3SyncLocalFilesystem


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with airflow.DAG(
    dag_id="generate_docs",
    schedule_interval="@daily",
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        env=env_vars,
        append_env=True,
    )

    dbt_generate_docs = bash.BashOperator(
        task_id="dbt_generate_docs",
        bash_command="dbt docs generate",
        env=env_vars,
        append_env=True,
    )

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket", bucket_name=Variable.get("S3_DOCS_BUCKET", "c2-dbt-docs"), aws_conn_id="s3_docs"
    )

    copy_files = S3SyncLocalFilesystem(
        task_id="copy_files",
        directory=os.getenv("DBT_TARGET_PATH"),
        dest_key_prefix="",
        dest_bucket=Variable.get("S3_DOCS_BUCKET", "c2-dbt-docs"),
        aws_conn_id="s3_docs",
        replace=True,
    )

    ([dbt_deps.as_setup(), create_bucket.as_setup()] >> dbt_generate_docs >> copy_files)
