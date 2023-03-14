import textwrap

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators import empty
from airflow.operators.python import get_current_context, task
from dags.common import db, slack

default_args = {
    'on_failure_callback': slack.task_fail_alert,
}

def to_buffer(df):
    from io import StringIO

    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)
    return buffer


def pg_store(table_name, df):
    from psycopg2 import sql

    with db.MetabaseDBCursor() as (cursor, conn):
        cursor.execute(
            sql.SQL(textwrap.dedent(
                """
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name}(
                    Date TIMESTAMP,
                    Recommandation DECIMAL(10,2),
                    Produit VARCHAR(512)
                );
                """
            )).format(table_name=sql.Identifier(table_name))
        )
        conn.commit()
        cursor.copy_from(to_buffer(df), table_name, sep=",")
        conn.commit()


with DAG(
    "nps_fetcher",
    start_date=pendulum.datetime(2023, 3, 21, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    @task(task_id="end")
    def end(**kwargs):
        slack.task_success_alert(get_current_context())

    @task(task_id="store_gsheets")
    def store_gsheets(**kwargs):
        import pandas as pd

        with db.MetabaseDBCursor() as (_, conn):
            df = pd.read_sql_query('SELECT * FROM "suivi_satisfaction";', conn)
            df.rename(columns={"Recommendation": "Recommandation"}, inplace=True)
            df["Produit"] = "Pilotage de l'inclusion"
            df = df[["Date", "Recommandation", "Produit"]]

        gip_nps_table_name = Variable.get("GIP_NPS_TABLE_NAME")  # "GIP_suivi_NPS"
        for name, pub_sheet_url in Variable.get("NPS_NAME_PUB_SHEET_URL_TUPLES", deserialize_json=True):
            print(f"reading {name=} at {pub_sheet_url=}")
            sheet_df = pd.read_csv(pub_sheet_url)
            sheet_df.rename(
                columns={
                    "Submitted at": "Date",
                    "Dates": "Date",
                    sheet_df.columns[-1]: "Recommandation",
                },
                inplace=True,
            )
            sheet_df["Produit"] = name
            sheet_df = sheet_df[["Date", "Recommandation", "Produit"]]
            df = pd.concat([df, sheet_df])

        pg_store(gip_nps_table_name, df)

    store_gsheet_task = store_gsheets()
    end_task = end()

    (start >> store_gsheet_task >> end_task)
