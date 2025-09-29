import re
from pathlib import Path

import pandas as pd
import sqlalchemy.types as types
from airflow import DAG
from airflow.decorators import task
from airflow.models import Param, Variable
from airflow.operators import bash
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.ext.declarative import declarative_base

from dags.common import airtable, db, dbt, default_dag_args, departments, gsheet, slack
from dags.common.monrecap.helpers import convert_date_columns
from dags.common.tasks import create_models


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

DB_SCHEMA = "monrecap"
variables_files = Path(__file__).parent / "common" / "monrecap" / "variables_barometre.csv"

# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase
MonrecapBase = declarative_base()

with DAG(
    "mon_recap",
    schedule="@daily",
    params={
        "drop_baro": Param(
            title="drop table raw_barometre",
            type="boolean",
            default=False,
            description="Drops the raw_barometre table. Use it when new columns are added to the file",
        ),
    },
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()
    date_pattern = re.compile(r"^date", re.IGNORECASE)

    variables = gsheet.get_variables(variables_files, delimiter_type=";")
    barometre_model = gsheet.build_data_model(  # Needs to be build before `create_models(MonrecapBase)`
        variables,
        DB_SCHEMA,
        MonrecapBase,
        primary_key="submission_id",
        tablename="raw_barometre",
        classname="MonRecapBaro",
    )

    @task.skip_if(lambda context: not context["params"]["drop_baro"])
    @task
    def drop_barometre_table():
        con = db.connection_engine()
        con.execute("""drop table if exists monrecap.raw_barometre cascade;""")

    @task
    def import_barometre(model, data_spec):
        data = gsheet.get_data_from_sheet(Variable.get("BAROMETRE_MON_RECAP"), data_spec)
        gsheet.insert_data_to_db(model, data)

    @task
    def monrecap_airtable(**kwargs):
        con = db.connection_engine()
        # Need to drop these tables and the views created with them in order to be able to run the table_data.to_sql()
        con.execute(
            """drop table if exists monrecap."Commandes_v0" cascade;
                    drop table if exists monrecap.barometre cascade;
                    drop table if exists monrecap.contacts_non_commandeurs_v0 cascade;
                    drop table if exists monrecap.Contacts_v0 cascade"""
        )

        table_mapping = {
            "Commandes": "Commandes_v0",
            "Contacts": "Contacts_v0",
            "Contacts non commandeurs": "contacts_non_commandeurs_v0",
        }

        for table_name in table_mapping:
            url, headers = airtable.connection_airtable(table_name)
            table_data = airtable.fetch_airtable_data(url, headers)

            if table_name == "Commandes":
                # getting all the columns starting with date + Submitted at when needed.
                # This is repeated for all imported tables
                commandes_dates = [
                    col for col in table_data.columns if date_pattern.match(col) or col == "Submitted at"
                ]
                convert_date_columns(table_data, commandes_dates)

                table_data["Nom Departement"] = table_data["Code Postal"].apply(
                    lambda cp: "-".join([item for item in departments.get_department(cp) if item is not None])
                )
            elif table_name == "Contacts":
                contacts_dates = [col for col in table_data.columns if date_pattern.match(col)]
                convert_date_columns(table_data, contacts_dates)
                table_data["Type de contact"] = "contact commandeur"
                # fixing Annaelle's double quotes, if you read this I'll get my revenge
                table_data = table_data.rename(
                    columns={
                        'Date envoi mail "Relance" J+71': "Date envoi mail Relance J+71",
                        'Envoi mail "merci"': "Envoi mail merci",
                        'Date envoi mail "merci"': "Date envoi mail merci",
                    }
                )

            elif table_name == "Contacts non commandeurs":
                contacts_non_commandeurs_dates = [col for col in table_data.columns if date_pattern.match(col)]
                convert_date_columns(table_data, contacts_non_commandeurs_dates)
                table_data["Type de contact"] = "contact non commandeur"

            db_table_name = table_mapping[table_name]

            dtype_mapping = {}
            for col in table_data.columns:
                # Check if column contains dict or list values
                sample_values = pd.concat([table_data[col].dropna().head(1000), table_data[col].dropna().tail(1000)])
                if any(isinstance(val, dict | list) for val in sample_values):
                    dtype_mapping[col] = types.JSON

            table_data.to_sql(
                db_table_name, con=con, schema=DB_SCHEMA, if_exists="replace", index=False, dtype=dtype_mapping
            )

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        trigger_rule=TriggerRule.ALL_DONE,
        env=env_vars,
        append_env=True,
    )

    dbt_seed = bash.BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed",
        env=env_vars,
        append_env=True,
    )

    dbt_monrecap = bash.BashOperator(
        task_id="dbt_monrecap",
        bash_command="dbt run --select monrecap",
        env=env_vars,
        append_env=True,
    )

    (
        drop_barometre_table()
        >> create_models(MonrecapBase)
        >> [monrecap_airtable(), import_barometre(barometre_model, variables)]
        >> dbt_deps
        >> dbt_seed
        >> dbt_monrecap
        >> slack.success_notifying_task()
    )
