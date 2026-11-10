import io
import logging

import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy
from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, Variable, task
from botocore.exceptions import ClientError
from sqlalchemy.dialects import postgresql

from dags.common import db, dbt, default_dag_args, s3, slack
from dags.common.tasks import create_schema


DB_SCHEMA = "raw_data_inclusion"
S3_PREFIX = "data/marts/"
TABLES = ("services", "structures")
LOOKBACK_HOURS = 48
STALE_AFTER_HOURS = 6
MIN_ROWS_RATIO = 0.5  # refuse to replace a table if the new file has less than half of its rows

logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {
    "default_args": dbt.get_default_args(),
    "start_date": pendulum.datetime(2026, 11, 6, tz="Europe/Paris"),
}


def marts_key(logical_date, name):
    # data·inclusion hourly runs write to data/marts/<Paris date>/scheduled__<UTC logical date>/v1/<name>.parquet
    paris_date = logical_date.in_timezone("Europe/Paris").date()
    return f"{S3_PREFIX}{paris_date}/scheduled__{logical_date.in_timezone('UTC').isoformat()}/v1/{name}.parquet"


def exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:  # without ListBucket, a missing key returns 403 instead of 404
        logger.info("%s: %s", key, e.response["Error"]["Code"])
        return False
    return True


def pg_type(arrow_type):
    if pa.types.is_list(arrow_type) and pa.types.is_string(arrow_type.value_type):
        return postgresql.ARRAY(sqlalchemy.Text)
    if pa.types.is_nested(arrow_type):
        return postgresql.JSONB
    return None


def read_table(client, bucket, key):
    table = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key=key)["Body"].read()))
    if not table.num_rows:
        raise ValueError(f"{key} is empty, keeping the existing data")

    df = table.to_pandas()
    dtype = {f.name: t for f in table.schema if (t := pg_type(f.type)) is not None}
    for col in dtype:
        df[col] = table.column(col).to_pylist()
    return df, dtype


def row_count(con, table_name):
    if not sqlalchemy.inspect(con).has_table(table_name, schema=DB_SCHEMA):
        return 0
    return con.execute(sqlalchemy.text(f"select count(*) from {DB_SCHEMA}.{table_name}")).scalar()


with DAG("data_inclusion", schedule="0 6 * * *", **dag_args) as dag:
    env_vars = db.connection_envvars()

    @task
    def latest_keys():
        client = s3.client("DATA_INCLUSION_S3")
        bucket = Variable.get("DATA_INCLUSION_S3_BUCKET_NAME")

        # Our key is not allowed to list the bucket: walk back the hourly runs until all files exist
        now = pendulum.now("UTC").start_of("hour")
        for hours in range(LOOKBACK_HOURS):
            keys = {name: marts_key(now.subtract(hours=hours), name) for name in TABLES}
            if all(exists(client, bucket, key) for key in keys.values()):
                logger.info("Latest files: %r", keys)
                if hours >= STALE_AFTER_HOURS:
                    slack._call_webhook(f":warning: data·inclusion : le dernier export date de {hours}h ({keys})")
                return keys
        raise FileNotFoundError(f"No complete export found in s3://{bucket}/{S3_PREFIX} in the last {LOOKBACK_HOURS}h")

    @task
    def import_tables(keys):
        client = s3.client("DATA_INCLUSION_S3")
        bucket = Variable.get("DATA_INCLUSION_S3_BUCKET_NAME")
        # Read and check every file before touching the database
        tables = {name: read_table(client, bucket, key) for name, key in keys.items()}

        # Single transaction: all tables are replaced, or none
        with db.connection_engine().begin() as con:
            for name, (df, _) in tables.items():
                if len(df) < (current := row_count(con, f"raw_di_{name}")) * MIN_ROWS_RATIO:
                    raise ValueError(f"{name}: {len(df)} rows vs {current} currently, keeping the existing data")
            for name, (df, dtype) in tables.items():
                # cascade: dependent dbt views would otherwise block the drop, they are rebuilt by dbt
                con.execute(sqlalchemy.text(f"drop table if exists {DB_SCHEMA}.raw_di_{name} cascade"))
                df.to_sql(f"raw_di_{name}", con=con, schema=DB_SCHEMA, index=False, dtype=dtype)
                logger.info("%s: %r rows loaded", name, len(df))

    dbt_build = bash.BashOperator(
        task_id="dbt_build",
        bash_command='dbt build --select "+tag:data-inclusion" --exclude-resource-type seed',
        env=env_vars,
        append_env=True,
    )

    (
        create_schema(DB_SCHEMA).as_setup()
        >> import_tables(latest_keys())
        >> dbt_build
        >> slack.success_notifying_task()
    )
