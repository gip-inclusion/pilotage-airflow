import sqlalchemy
from airflow import DAG
from airflow.decorators import task
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
            """drop table if exists monrecap."Commandes" cascade;
                    drop table if exists monrecap.barometre cascade;"""
        )

        tables = ["Commandes", "Contacts", "Baromètre - Airflow"]
        for table_name in tables:
            url, headers = airtable.connection_airtable(table_name)
            df = airtable.fetch_airtable_data(url, headers)

            if table_name == "Commandes":
                df["Submitted at"] = pd.to_datetime(df["Submitted at"])
                df["Nom Departement"] = df["Code Postal"].apply(
                    lambda cp: "-".join([item for item in departments.get_department(cp) if item is not None])
                )
            if table_name == "Contacts":
                df["Date de première commande"] = pd.to_datetime(df["Date de première commande"])
                df["Date de dernière commande"] = pd.to_datetime(df["Date de dernière commande"])
            elif table_name == "Baromètre - Airflow":
                df["Submitted at"] = pd.to_datetime(df["Submitted at"])
                table_name = "barometre"

            df.to_sql(
                table_name,
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

    (start >> monrecap_airtable >> dbt_deps >> dbt_seed >> dbt_monrecap >> end)
