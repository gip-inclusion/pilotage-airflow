import json
import re
from pathlib import Path

import sqlalchemy.types as types
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import bash

from dags.common import airtable, db, dbt, default_dag_args, departments, gsheet
from dags.common.monrecap.helpers import convert_date_columns
from dags.common.monrecap.models import DB_SCHEMA, MonRecap, create_tables


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

VARIABLES_FILE = Path("/opt/airflow/dags/common/monrecap/variables_barometre.csv")
delimiter_type = ";"
primary_key = "submissionid"
tablename = "raw_barometre"
classname = "MonRecapBaro"

with DAG("mon_recap", schedule_interval="10 0 * * *", **dag_args) as dag:

    env_vars = db.connection_envvars()

    date_pattern = re.compile(r"^date", re.IGNORECASE)

    @task(task_id="create_table")
    def create_table(variables):
        create_tables(variables)

    @task(task_id="import_mon_recap_baro")
    def import_monrecap_baro(variables):
        monrecap_baro_model = gsheet.build_data_model(
            variables, primary_key, tablename, DB_SCHEMA, classname, MonRecap
        )
        data = gsheet.get_data_from_sheet(Variable.get("BAROMETRE_MON_RECAP"), variables)
        gsheet.insert_data_to_db(monrecap_baro_model, data)

    variables = gsheet.get_variables(VARIABLES_FILE, delimiter_type)

    @task(task_id="import_mon_recap_airtable")
    def import_monrecap_airtable(**kwargs):

        con = db.connection_engine()
        # Need to drop these tables and the views created with them in order to be able to run the df.to_sql()
        con.execute(
            """drop table if exists monrecap."Commandes_v0" cascade;
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
                convert_date_columns(df, commandes_dates)

                df["Nom Departement"] = df["Code Postal"].apply(
                    lambda cp: "-".join([item for item in departments.get_department(cp) if item is not None])
                )
            elif table_name == "Contacts":
                contacts_dates = [col for col in df.columns if date_pattern.match(col)]
                convert_date_columns(df, contacts_dates)
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
                convert_date_columns(df, contacts_non_commandeurs_dates)
                df["Type de contact"] = "contact non commandeur"

            db_table_name = table_mapping[table_name]

            dtype_mapping = {}
            # we convert the dict to string in order to be able to import the data
            for col in df.columns:
                dict_data = df[col].apply(lambda x: isinstance(x, (dict, list))).any()
                if dict_data:
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
                    )
                    dtype_mapping[col] = types.JSON

            df.to_sql(db_table_name, con=con, schema=DB_SCHEMA, if_exists="replace", index=False, dtype=dtype_mapping)

    import_monrecap_airtable = import_monrecap_airtable()

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
        create_table(variables).as_setup()
        >> [import_monrecap_airtable, import_monrecap_baro(variables)]
        >> dbt_deps
        >> dbt_seed
        >> dbt_monrecap
    )
