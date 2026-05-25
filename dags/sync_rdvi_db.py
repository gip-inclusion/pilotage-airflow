import contextlib
import os
import subprocess
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.ssh.hooks import ssh
from furl import furl

from dags.common import db, default_dag_args


DAG_ID = "sync_rdvi_raw_schema"

SOURCE_SCHEMA = "rdvi"
TARGET_SCHEMA = "raw_rdvi"

LOCAL_TMP_ROOT = Path("/tmp") / DAG_ID


@contextlib.contextmanager
def source_tunnel_db_url():
    db_url = furl(Variable.get("RDVI_DB_URL_SECRET"))
    db_url.scheme = "postgresql"

    ssh_hook = ssh.SSHHook(ssh_conn_id="rdvi_scalingo_ssh")
    tunnel = ssh_hook.get_tunnel(
        remote_host=db_url.host,
        remote_port=db_url.port,
    )

    print("START ssh_tunnel")
    tunnel.start()

    try:
        db_url.host = "127.0.0.1"
        db_url.port = tunnel.local_bind_port

        print(f"{tunnel.local_bind_port=}")
        print("END ssh_tunnel")

        yield db_url.url

    finally:
        print("START ssh_tunnel_stop")
        tunnel.stop()
        print("END ssh_tunnel_stop")


def run_command(command_name, command, env=None):
    print(f"START {command_name}")

    subprocess.run(
        command,
        env=os.environ | (env or {}),
        check=True,
    )

    print(f"END {command_name}")


def create_restore_script(dump_path, restore_script_path):
    print("START create_restore_script")

    restore_script_path.write_text(
        f"""
DROP SCHEMA IF EXISTS "{TARGET_SCHEMA}" CASCADE;
DROP SCHEMA IF EXISTS "{SOURCE_SCHEMA}" CASCADE;

\\i '{dump_path}'

ALTER SCHEMA "{SOURCE_SCHEMA}" RENAME TO "{TARGET_SCHEMA}";
"""
    )

    print("END create_restore_script")


with DAG(
    dag_id=DAG_ID,
    schedule="0 5 * * *",
    **default_dag_args(),
) as dag:

    @task
    def refresh_raw_schema(**context):
        print("START refresh_raw_schema")
        print(f"{SOURCE_SCHEMA=}")
        print(f"{TARGET_SCHEMA=}")

        run_id = context["run_id"].replace(":", "_")
        base_tmp = LOCAL_TMP_ROOT / run_id
        base_tmp.mkdir(parents=True, exist_ok=True)

        dump_path = base_tmp / "rdvi_dump.sql"
        restore_script_path = base_tmp / "restore_rdvi_as_raw_rdvi.sql"

        with source_tunnel_db_url() as source_db_url:
            run_command(
                command_name="pg_dump_source_schema",
                command=[
                    "pg_dump",
                    source_db_url,
                    "--schema",
                    SOURCE_SCHEMA,
                    "--no-owner",
                    "--no-privileges",
                    "--file",
                    str(dump_path),
                ],
            )

        create_restore_script(
            dump_path=dump_path,
            restore_script_path=restore_script_path,
        )

        run_command(
            command_name="psql_restore_schema_to_target",
            command=[
                "psql",
                "--set",
                "ON_ERROR_STOP=1",
                "--single-transaction",
                "--file",
                str(restore_script_path),
            ],
            env=db.connection_envvars(),
        )

        print("END refresh_raw_schema")

    refresh_raw_schema()
