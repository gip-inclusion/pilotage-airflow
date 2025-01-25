import sqlalchemy
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import bash, empty

from dags.common import airtable, db, dbt, default_dag_args, departments, slack


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}
DB_SCHEMA = "monrecap"

with DAG("mon_recap", schedule_interval="@daily", **dag_args) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    env_vars = db.connection_envvars()

    @task(task_id="monrecap_airtable")
    def monrecap_airtable(**kwargs):
        import pandas as pd

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
        tables = list(table_mapping.values())
        for table_name in tables:
            url, headers = airtable.connection_airtable(table_name)
            df = airtable.fetch_airtable_data(url, headers)

            if table_name == "Commandes":
                df["Submitted at"] = pd.to_datetime(df["Submitted at"])
                df["Nom Departement"] = df["Code Postal"].apply(
                    lambda cp: "-".join([item for item in departments.get_department(cp) if item is not None])
                )
            elif table_name == "Contacts":
                # fixing Annaelle's double quotes, if you read this I'll get my revenge
                df = df.rename(
                    columns={
                        'Date envoi mail "Relance" J+71': "Date envoi mail Relance J+71",
                        'Envoi mail "merci"': "Envoi mail merci",
                        'Date envoi mail "merci"': "Date envoi mail merci",
                    }
                )
                df["Date de première commande"] = pd.to_datetime(df["Date de première commande"])
                df["Date de dernière commande"] = pd.to_datetime(df["Date de dernière commande"])
                df["Type de contact"] = "contact commandeur"

            elif table_name == "Contacts non commandeurs":
                df["Date de première commande"] = pd.to_datetime(df["Date de première commande"])
                df["Date de dernière commande"] = pd.to_datetime(df["date de la dernière commande"])
                df["Type de contact"] = "contact non commandeur"

            db_table_name = table_mapping[table_name]

            df.to_sql(
                db_table_name,
                con=con,
                schema=DB_SCHEMA,
                if_exists="replace",
                index=False,
                dtype={
                    "Validation manuelle": sqlalchemy.types.JSON,
                    "Demande de prise de RDV": sqlalchemy.types.JSON,
                },
            )

    monrecap_airtable = monrecap_airtable()

    # Tally handle poorly the import to airtable, therefore we used a gsheet instead
    @task(task_id="monrecap_gsheet")
    def monrecap_gsheet(**kwargs):
        import pandas as pd

        sheet_url = Variable.get("BAROMETRE_MON_RECAP")
        print(f"reading barometre mon recap at {sheet_url=}")
        df = pd.read_csv(sheet_url)
        df.drop_duplicates()  # if the synch of the gsheet is forced, duplicates will be created
        df["Submitted at"] = pd.to_datetime(df["Submitted at"])
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
