import os
import shutil
import subprocess
from pathlib import Path

from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, task

from dags.common import db, dbt, default_dag_args


DAG_ID = "sync_rdvi_raw_schema"

SOURCE_SCHEMA = "rdvi"
TARGET_SCHEMA = "raw_rdvi"

LOCAL_TMP_ROOT = Path("/tmp") / DAG_ID

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


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
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    @task
    def refresh_raw_schema(**context):
        print("START refresh_raw_schema")
        print(f"{SOURCE_SCHEMA=}")
        print(f"{TARGET_SCHEMA=}")

        run_id = context["run_id"].replace(":", "_")
        base_tmp = LOCAL_TMP_ROOT / run_id
        base_tmp.mkdir(parents=True, exist_ok=True)

        try:
            dump_path = base_tmp / "rdvi_dump.sql"
            restore_script_path = base_tmp / "restore_rdvi_as_raw_rdvi.sql"

            with db.tunnel_db_url("RDVI_DB_URL_SECRET", ssh_conn_id="rdvi_scalingo_ssh") as source_db_url:
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

        finally:
            print("START cleanup_tmp_files")
            shutil.rmtree(base_tmp, ignore_errors=True)
            print("END cleanup_tmp_files")

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
        bash_command='dbt build --select "+tag:rdv-insertion"',
        env=env_vars,
        append_env=True,
    )

    refreshed = refresh_raw_schema()

    refreshed >> dbt_debug >> dbt_deps >> dbt_build
