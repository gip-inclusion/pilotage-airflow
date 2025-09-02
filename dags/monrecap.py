import re

import pandas as pd
import sqlalchemy.types as types
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import bash

from dags.common import airtable, db, dbt, default_dag_args, departments, monrecap, slack
from dags.common.tasks import create_schema


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}
DB_SCHEMA = "monrecap"

with DAG("mon_recap", schedule="@daily", **dag_args) as dag:
    env_vars = db.connection_envvars()

    date_pattern = re.compile(r"^date", re.IGNORECASE)

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
                monrecap.convert_date_columns(table_data, commandes_dates)

                table_data["Nom Departement"] = table_data["Code Postal"].apply(
                    lambda cp: "-".join([item for item in departments.get_department(cp) if item is not None])
                )
            elif table_name == "Contacts":
                contacts_dates = [col for col in table_data.columns if date_pattern.match(col)]
                monrecap.convert_date_columns(table_data, contacts_dates)
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
                monrecap.convert_date_columns(table_data, contacts_non_commandeurs_dates)
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

    # Tally handle poorly the import to airtable, therefore we used a gsheet instead
    @task
    def monrecap_gsheet(**kwargs):
        import pandas as pd

        sheet_url = Variable.get("BAROMETRE_MON_RECAP")
        print(f"reading barometre mon recap at {sheet_url=}")
        sheet_data = pd.read_csv(sheet_url)
        sheet_data.drop_duplicates()  # if the synch of the gsheet is forced, duplicates will be created
        sheet_data = sheet_data.rename(
            columns={
                "Avez-vous constaté que vos usagers utilisent le carnet avec leurs autres accompagnateurs ?": (
                    "Carnets utilisés avec d'autres accompagnateurs ?"
                ),
                "Avez-vous constaté que vos usagers utilisent le carnet avec leurs autres accompagnateurs ?.1": (
                    "Carnets utilisés avec d'autres accompagnateurs ? (chiffres)"
                ),
                "Quel(s) type(s) de public accompagnez-vous ? (Personnes en situation d'illectronisme )": (
                    "Quel(s) type(s) de public accompagnez-vous ? (illectronisme)"
                ),
                "Quel(s) type(s) de public accompagnez-vous ? (Personnes en situation d'illetrisme )": (
                    "Quel(s) type(s) de public accompagnez-vous ? (illetrisme)"
                ),
                "D'après vous, pourquoi les usagers ne l'utilisent pas avec d'autres professionnels ?": (
                    "Pourquoi les usagers ne l'utilisent pas avec d'autres professionnels ?"
                ),
                "D'après vous, pourquoi les usagers ne l'utilisent pas avec d'autres professionnels ?.1": (
                    "(bis) Pourquoi les usagers ne l'utilisent pas avec d'autres professionnels ?"
                ),
                "Votre adresse mail ": "Votre adresse mail ?",
            }
        )
        baro_dates = [col for col in sheet_data.columns if date_pattern.match(col) or col == "Submitted at"]
        monrecap.convert_date_columns(sheet_data, baro_dates)
        sheet_data.to_sql(
            "barometre_v0", con=db.connection_engine(), schema=DB_SCHEMA, if_exists="replace", index=False
        )

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
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
        create_schema(DB_SCHEMA).as_setup()
        >> monrecap_airtable()
        >> monrecap_gsheet()
        >> dbt_deps
        >> dbt_seed
        >> dbt_monrecap
        >> slack.success_notifying_task()
    )
