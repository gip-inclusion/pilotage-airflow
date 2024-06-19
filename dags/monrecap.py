from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import airtable, db_monrecap, default_dag_args, slack


with DAG(
    "mon_recap",
    schedule_interval="@daily",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="monrecap_airtable")
    def monrecap_airtable(**kwargs):
        import pandas as pd

        tables = ["Commandes", "Contacts"]
        for table_name in tables:
            url, headers = airtable.connection_airtable(table_name)
            df = airtable.fetch_airtable_data(url, headers)
            if table_name == "Commandes":
                df["Submitted at"] = pd.to_datetime(df["Submitted at"])
            if table_name == "Contacts":
                df["Date de première commande"] = pd.to_datetime(df["Date de première commande"])
                df["Date de dernière commande"] = pd.to_datetime(df["Date de dernière commande"])
            df.to_sql(
                table_name,
                con=db_monrecap.connection_engine_monrecap(),
                if_exists="replace",
                index=False,
            )

    monrecap_airtable = monrecap_airtable()

    @task(task_id="monrecap_gsheet")
    def monrecap_gsheet(**kwargs):
        import pandas as pd

        sheet_url = Variable.get("BAROMETRE_MON_RECAP")
        print(f"reading barometre monrecap at {sheet_url=}")
        df_baro = pd.read_csv(sheet_url)
        df_baro["Submitted at"] = pd.to_datetime(df_baro["Submitted at"])
        df_baro.to_sql("barometre", con=db_monrecap.connection_engine_monrecap(), if_exists="replace", index=False)

    monrecap_gsheet = monrecap_gsheet()

    (start >> monrecap_airtable >> monrecap_gsheet >> end)
