import logging
import mimetypes
import os
import pathlib

import airflow
from airflow.models import Variable
from airflow.operators import bash
from airflow.operators.python import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

from dags.common import db, dbt, default_dag_args


logger = logging.getLogger(__name__)

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
        bash_command="dbt docs generate --static",
        env=env_vars,
        append_env=True,
    )

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket", bucket_name=Variable.get("S3_DOCS_BUCKET", "c2-dbt-docs"), aws_conn_id="s3_docs"
    )

    @task
    def copy_file():
        filename = pathlib.Path(os.getenv("DBT_TARGET_PATH")) / "static_index.html"
        s3_bucket, s3_key = Variable.get("S3_DOCS_BUCKET", "c2-dbt-docs"), "index.html"
        logger.info(f"Copying {filename} to s3://{s3_bucket}/{s3_key}")
        s3_hook = S3Hook(
            aws_conn_id="s3_docs",
            extra_args={"ContentType": mimetypes.guess_type(filename)[0] or "binary/octet-stream"},
        )
        s3_hook.load_file(
            filename,
            s3_key,
            s3_bucket,
            replace=True,
            acl_policy="public-read",
        )

    ([dbt_deps.as_setup(), create_bucket.as_setup()] >> dbt_generate_docs >> copy_file())
