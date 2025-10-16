import re
import time
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from pyairtable import Table
from sqlalchemy.ext.declarative import declarative_base

from dags.common import db, dbt, default_dag_args, gsheet, slack
from dags.common.tasks import create_models


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase
Base = declarative_base()


with DAG(
    "mon_recap_mailing_autres_pro_aap",
    schedule="@daily",
    **dag_args,
) as dag:
    variables = gsheet.get_variables(
        Path(__file__).parent / "common" / "monrecap" / "variables_mailing.csv", delimiter_type=";"
    )
    monrecap_mailing = gsheet.build_data_model(
        variables,
        "monrecap",
        Base,
        primary_key="submission_id",
        tablename="mailing_list_others_aap",
        classname="MonRecapMailing",
    )

    @task
    def explode_gsheet(data_spec):
        # read xls and rename columns
        df = pd.read_excel(Variable.get("MON_RECAP_MAILING"))
        variables_dict = {variable_info["question"]: variable for variable, variable_info in data_spec.items()}

        df = df.astype("object")
        df = df.where(pd.notna(df), None)
        df = df.rename(columns=variables_dict)
        df = df[variables_dict.values()]

        df["email_pro"] = (
            df["email_pro"]
            .astype(str)
            .apply(lambda x: re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", x))
        )
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

    @task
    def drop_mailing_table():
        with db.connection_engine().connect() as con:
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
            Variable.get("MON_RECAP_AIRTABLE_TOKEN_API"),
            Variable.get("MON_RECAP_AIRTABLE_BASE_ID"),
            "Mailing autres pros AAP",
        )

        existing_records = table.all(fields=["submission_id"])
        existing_ids = {r["fields"].get("submission_id") for r in existing_records if r["fields"].get("submission_id")}
        print(f"{len(existing_ids)} rows are already present in airtable")

        # Upload in batches (max 10 records per request)
        batch_size = 10
        uploaded = 0

        df_append = df[~df["submission_id"].isin(existing_ids)]
        print(f"{len(df_append)} new rows are added to airtable ")

        records = df_append.to_dict("records")

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            table.batch_create(batch)
            uploaded += len(batch)

            # Respect rate limits
            if i + batch_size < len(records):
                time.sleep(0.2)

    df = explode_gsheet(variables)
    (
        drop_mailing_table()
        >> create_models(Base)
        >> [import_mailing(monrecap_mailing, df), export_data_to_airtable(df)]
        >> slack.success_notifying_task()
    )
