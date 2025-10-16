import re
import time
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Param, Variable
from pyairtable import Table
from sqlalchemy.ext.declarative import declarative_base

from dags.common import db, dbt, default_dag_args, gsheet, slack
from dags.common.tasks import create_models


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

DB_SCHEMA = "monrecap"
variables_files = Path(__file__).parent / "common" / "monrecap" / "variables_mailing.csv"

# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase
MonrecapBase = declarative_base()

with DAG(
    "mon_recap_mailing_autres_pro_aap",
    schedule="@daily",
    params={
        "drop_mailing": Param(
            title="drop table mailing_list_others_aap",
            type="boolean",
            default=False,
            description="Drops the mailing_list_others_aap table. Use it when new columns are added to the file",
        ),
    },
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()
    email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

    airtable_table_name = "Mailing autres pros AAP"

    variables = gsheet.get_variables(variables_files, delimiter_type=";")
    barometre_model = gsheet.build_data_model(
        variables,
        DB_SCHEMA,
        MonrecapBase,
        primary_key="submission_id",
        tablename="mailing_list_others_aap",
        classname="MonRecapMailing",
    )

    @task
    def explode_gsheet(data_spec):
        # needed to use the xls because there are commas in the file
        df = gsheet.get_data_from_xls(Variable.get("MAILING_MON_RECAP"), data_spec)
        df["email_pro"] = df["email_pro"].astype(str).apply(lambda x: re.findall(email_regex, x))
        df_exploded = df.explode("email_pro")
        df_exploded["email_pro"] = df_exploded["email_pro"].fillna("")

        df_result = df_exploded[
            ["submission_id", "respondentid", "datesubmission", "email_structure", "structure", "email_pro"]
        ].copy()

        df_result["submission_id"] = (
            (df_result.groupby("submission_id").cumcount() + 1).astype(str) + "_" + df_result["submission_id"]
        )
        print(df_result.columns)
        return df_result

    @task.skip_if(lambda context: not context["params"]["drop_mailing"])
    @task
    def drop_mailing_table():
        con = db.connection_engine()
        con.execute("""drop table if exists monrecap.mailing_list_others_aap cascade;""")

    @task
    def import_mailing(model, df):
        print(df.columns)
        gsheet.insert_data_to_db(model, df)

    @task
    def export_data_to_airtable(df):
        df["datesubmission"] = df["datesubmission"].dt.strftime("%Y-%m-%d")  # match airtable date format
        print(df.columns)

        # connect to airtable
        table = Table(
            Variable.get("TOKEN_API_AIRTABLE_MON_RECAP"),
            Variable.get("BASE_ID_AIRTABLE_MON_RECAP"),
            airtable_table_name,
        )

        try:
            existing_records = table.all(fields=["submission_id"])
            existing_ids = {
                r["fields"].get("submission_id") for r in existing_records if r["fields"].get("submission_id")
            }
            print(f"{len(existing_ids)} rows are already present in airtable")
        except Exception as e:
            raise AirflowFailException(f"Failed to gather airtable data : {e}") from e

        # Upload in batches (max 10 records per request)
        batch_size = 10
        uploaded = 0

        df_append = df[~df["submission_id"].isin(existing_ids)]
        print(f"{len(df_append)} new rows are added to airtable ")

        records = df_append.to_dict("records")

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            try:
                table.batch_create(batch)
                uploaded += len(batch)

                # Respect rate limits
                if i + batch_size < len(records):
                    time.sleep(0.2)
            except Exception as e:
                raise AirflowFailException(f"Error uploading batch: {e}") from e

    df = explode_gsheet(variables)
    (
        drop_mailing_table()
        >> create_models(MonrecapBase)
        >> [import_mailing(barometre_model, df), export_data_to_airtable(df)]
        >> slack.success_notifying_task()
    )
