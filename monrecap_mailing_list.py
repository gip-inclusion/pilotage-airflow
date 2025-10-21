import os
import re
import time

import pandas as pd
import sqlalchemy as sqla
from prefect import flow, task
from pyairtable import Table as AirtableTable
from sqlalchemy.ext.declarative import declarative_base

from dags.common import db, dbt, default_dag_args, gsheet


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

Base = declarative_base()


def match_emails(string):
    return re.findall(
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        string,
        flags=re.IGNORECASE,
    )


class MonRecapMailing(Base):
    __tablename__ = "mailing_list_others_aap"
    __table_args__ = {"schema": "monrecap"}

    submission_id = sqla.Column(sqla.String, primary_key=True)
    respondentid = sqla.Column(sqla.String)
    datesubmission = sqla.Column(sqla.Date)
    email_structure = sqla.Column(sqla.String)
    structure = sqla.Column(sqla.String)
    email_pro = sqla.Column(sqla.String)


@task()
def group_by_submission_ids():
    df_gsheet = pd.read_excel(os.environ.get("MAILING_MON_RECAP"))  # is Excel really necessary ?

    column_question_mapping = {
        "Submission ID": "submission_id",
        "Submitted at": "datesubmission",
        "Respondent ID": "respondentid",
        "Votre adresse mail": "email_structure",
        "Votre structure": "structure",
        "Liste des adresse mails des professionnels qui vont utiliser le carnet (séparé par une virgule)": "email_pro",
    }

    df_gsheet = df_gsheet.astype("object")
    df_gsheet = df_gsheet.where(pd.notna(df_gsheet), None)
    df_gsheet = df_gsheet.rename(columns=column_question_mapping)
    df_gsheet = df_gsheet[column_question_mapping.values()]

    df_gsheet["email_pro"] = df_gsheet["email_pro"].astype(str).apply(match_emails)
    df_exploded = df_gsheet.explode("email_pro")
    df_exploded["email_pro"] = df_exploded["email_pro"].fillna("")

    df_result = df_exploded.copy()  # really necessary ?

    df_result["submission_id"] = (
        (df_result.groupby("submission_id").cumcount() + 1).astype(str) + "_" + df_result["submission_id"]
    )
    print(df_result.columns)
    return df_result


@task()
def export_data_to_airtable(df):
    df["datesubmission"] = df["datesubmission"].dt.strftime("%Y-%m-%d")  # match airtable date format
    print(df.columns)

    airtable_table = AirtableTable(
        os.environ.get("TOKEN_API_AIRTABLE_MON_RECAP"),
        os.environ.get("BASE_ID_AIRTABLE_MON_RECAP"),
        "Mailing autres pros AAP",
    )

    existing_records = airtable_table.all(fields=["submission_id"])
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
        airtable_table.batch_create(batch)
        uploaded += len(batch)

        # Respect rate limits
        if i + batch_size < len(records):
            time.sleep(0.2)


@task()
def drop_mailing_table():
    with db.connection_engine().connect() as connection:
        connection.execute("""
            DROP TABLE IF EXISTS monrecap.mailing_list_others_aap CASCADE;
        """)


@task(cache_policy=None)
def import_mailing(model, df):
    print(df.columns)
    gsheet.insert_data_to_db(model, df)


@flow(
    name="MonRecap Mailing - Autres Pro AAP",
    description="Process and import mailing list data for MonRecap",
    retries=1,
    retry_delay_seconds=300,
)
def monrecap_mailing_autres_pro_aap():
    df = group_by_submission_ids()

    MonRecapMailing.metadata.create_all(db.connection_engine())
    import_mailing(MonRecapMailing, df)


monrecap_mailing_autres_pro_aap()
