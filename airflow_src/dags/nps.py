import textwrap
from io import StringIO

import pandas as pd
import pendulum
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

default_args = {}


def to_buffer(df):
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)
    return buffer


class MetabaseDBCursor:
    def __init__(self):
        self.cursor = None
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(
            host=Variable.get("PGHOST"),
            port=Variable.get("PGPORT"),
            dbname=Variable.get("PGDATABASE"),
            user=Variable.get("PGUSER"),
            password=Variable.get("PGPASSWORD"),
        )
        self.cursor = self.connection.cursor()
        return self.cursor, self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def pg_store(table_name, df):
    with MetabaseDBCursor() as (cursor, conn):
        cursor.execute(
            textwrap.dedent(
                f"""
                DROP TABLE IF EXISTS GIP_suivi_NPS;
                CREATE TABLE {table_name}(
                    Date TIMESTAMP,
                    Recommandation DECIMAL(10,2),
                    Produit VARCHAR(512)
                );
                """
            )
        )
        conn.commit()
        cursor.copy_from(to_buffer(df), table_name, sep=",")
        conn.commit()


with DAG(
    "nps_fetcher",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    @task(task_id="store_gsheets")
    def store_gsheets(ds=None, **kwargs):
        with MetabaseDBCursor() as (_, conn):
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
                    df.columns[-1]: "Recommandation",
                },
                inplace=True,
            )
            sheet_df["Produit"] = name
            sheet_df = df[["Date", "Recommandation", "Produit"]]
            df = pd.concat([df, sheet_df])

        pg_store(gip_nps_table_name, df)

    store_gsheet_task = store_gsheets()

    (start >> store_gsheet_task >> end)
