import re

import sqlalchemy.types as types
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import bash, empty

from dags.common import airtable, db, dbt, default_dag_args, departments, monrecap, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}
DB_SCHEMA = "monrecap"

with DAG("mon_recap", schedule_interval="@daily", **dag_args) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    env_vars = db.connection_envvars()

    date_pattern = re.compile(r"^date", re.IGNORECASE)

    @task(task_id="monrecap_airtable")
    def monrecap_airtable(**kwargs):

        con = db.connection_engine()
        # Need to drop these tables and the views created with them in order to be able to run the df.to_sql()
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

        for table_name in table_mapping.keys():
            url, headers = airtable.connection_airtable(table_name)
            df = airtable.fetch_airtable_data(url, headers)

            if table_name == "Commandes":
                # getting all the columns starting with date + Submitted at when needed.
                # This is repeated for all imported tables
                commandes_dates = [col for col in df.columns if date_pattern.match(col) or col == "Submitted at"]
                monrecap.convert_date_columns(df, commandes_dates)

                df["Nom Departement"] = df["Code Postal"].apply(
                    lambda cp: "-".join([item for item in departments.get_department(cp) if item is not None])
                )
            elif table_name == "Contacts":
                contacts_dates = [col for col in df.columns if date_pattern.match(col)]
                monrecap.convert_date_columns(df, contacts_dates)
                df["Type de contact"] = "contact commandeur"
                # fixing Annaelle's double quotes, if you read this I'll get my revenge
                df = df.rename(
                    columns={
                        'Date envoi mail "Relance" J+71': "Date envoi mail Relance J+71",
                        'Envoi mail "merci"': "Envoi mail merci",
                        'Date envoi mail "merci"': "Date envoi mail merci",
                    }
                )

            elif table_name == "Contacts non commandeurs":
                contacts_non_commandeurs_dates = [col for col in df.columns if date_pattern.match(col)]
                monrecap.convert_date_columns(df, contacts_non_commandeurs_dates)
                df["Type de contact"] = "contact non commandeur"

            db_table_name = table_mapping[table_name]

            dtype_mapping = {}
            for col in df.columns:
                # Check if column contains dict or list values
                sample_values = df[col].dropna().head(10)
                if any(isinstance(val, (dict, list)) for val in sample_values):
                    dtype_mapping[col] = types.JSON

            df.to_sql(db_table_name, con=con, schema=DB_SCHEMA, if_exists="replace", index=False, dtype=dtype_mapping)

    monrecap_airtable = monrecap_airtable()

    # Tally handle poorly the import to airtable, therefore we used a gsheet instead
    @task(task_id="monrecap_gsheet")
    def monrecap_gsheet(**kwargs):
        import pandas as pd

        sheet_url = Variable.get("BAROMETRE_MON_RECAP")
        print(f"reading barometre mon recap at {sheet_url=}")
        df = pd.read_csv(sheet_url)
        df.drop_duplicates()  # if the synch of the gsheet is forced, duplicates will be created
        df = df.rename(
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
        baro_dates = [col for col in df.columns if date_pattern.match(col) or col == "Submitted at"]
        monrecap.convert_date_columns(df, baro_dates)
        df.to_sql("barometre_v0", con=db.connection_engine(), schema=DB_SCHEMA, if_exists="replace", index=False)

    monrecap_gsheet = monrecap_gsheet()

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

    (start >> monrecap_airtable >> monrecap_gsheet >> dbt_deps >> dbt_seed >> dbt_monrecap >> end)
