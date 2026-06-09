import os
import shutil
import subprocess
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

from dags.common import db, default_dag_args


DAG_ID = "sync_marche_raw_schema"

SOURCE_SCHEMA = "public"
TEMP_SCHEMA = "raw_marche_tmp"
TARGET_SCHEMA = "raw_marche"

LOCAL_TMP_ROOT = Path("/tmp") / DAG_ID


def run_command(command_name, command, env=None):
    print(f"START {command_name}")

    result = subprocess.run(
        command,
        env=os.environ | (env or {}),
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    result.check_returncode()

    print(f"END {command_name}")


def rename_schema_in_dump(dump_path):
    source = SOURCE_SCHEMA
    target = TEMP_SCHEMA
    # The body of a COPY block is real data and may legitimately contain the text
    # "public."", so the schema
    # rewrite must never touch it — only DDL statements and the COPY header line.
    copy_data_block = r"/^COPY .* FROM stdin;$/,/^\\\.$/"
    run_command(
        "rename_schema_in_dump",
        [
            "sed",
            "-i",
            "-e",
            f"s/^CREATE SCHEMA {source};$/CREATE SCHEMA IF NOT EXISTS {target};/",
            # redirect every "public."-qualified object to the temp schema, outside COPY data
            "-e",
            f"{copy_data_block}!s/\\b{source}\\./{target}./g",
            # the COPY header sits on the first line of the data block, so handle it on its own
            "-e",
            f"s/^COPY {source}\\./COPY {target}./",
            # drop the PostGIS dependency entirely: store geometry/geography columns as
            # plain text (the typmod form, e.g. "geometry(Point,4326)", must be matched first)
            "-e",
            f"s/{target}\\.geometry([^)]*)/text/g",
            "-e",
            f"s/{target}\\.geography([^)]*)/text/g",
            "-e",
            f"s/{target}\\.geometry/text/g",
            "-e",
            f"s/{target}\\.geography/text/g",
            str(dump_path),
        ],
    )


def create_restore_script(dump_path, restore_script_path):
    print("START create_restore_script")

    restore_script_path.write_text(
        f"""
DROP SCHEMA IF EXISTS "{TARGET_SCHEMA}" CASCADE;
DROP SCHEMA IF EXISTS "{TEMP_SCHEMA}" CASCADE;
CREATE SCHEMA "{TEMP_SCHEMA}";

\\i '{dump_path}'

ALTER SCHEMA "{TEMP_SCHEMA}" RENAME TO "{TARGET_SCHEMA}";
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

        try:
            dump_path = base_tmp / "marche_dump.sql"
            restore_script_path = base_tmp / "restore_public_as_raw_marche.sql"

            with db.tunnel_db_url("MARCHE_DB_URL_SECRET") as source_db_url:
                run_command(
                    command_name="pg_dump_source_schema",
                    command=[
                        "pg_dump",
                        source_db_url,
                        "--schema",
                        SOURCE_SCHEMA,
                        "--no-owner",
                        "--no-privileges",
                        "--exclude-table=spatial_ref_sys",
                        # tables + data only
                        "--section=pre-data",
                        "--section=data",
                        "--file",
                        str(dump_path),
                    ],
                )

            rename_schema_in_dump(dump_path)

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

    refresh_raw_schema()
