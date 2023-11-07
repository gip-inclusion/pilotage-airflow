from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import db, default_dag_args, slack


NPS_CREATE_TABLE_SQL = """
DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name}(
    "Date" TIMESTAMP,
    "Recommandation" DECIMAL(10,2),
    "Produit" VARCHAR(512)
);
"""

with DAG(
    "nps_gip",
    schedule_interval="@daily",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="store_gsheets")
    def store_gsheets(**kwargs):
        import pandas as pd

        with db.MetabaseDBCursor() as (_, conn):
            df = pd.read_sql_query('SELECT * FROM "suivi_satisfaction";', conn)
            df.rename(columns={"Recommendation": "Recommandation"}, inplace=True)
            df["Produit"] = "Pilotage de l'inclusion"
            df = df[["Date", "Recommandation", "Produit"]]

        for name, pub_sheet_url in Variable.get("NPS_NAME_PUB_SHEET_URL_TUPLES", deserialize_json=True):
            print(f"reading {name=} at {pub_sheet_url=}")
            sheet_df = pd.read_csv(pub_sheet_url)
            column_commu = "Quelle est la probabilité que vous recommandiez"
            column_commu += " La communauté de l'inclusion à un collègue, partenaire ou homologue ?"

            # Identify the last column that contains an integer
            last_integer_column = None
            for column in sheet_df.columns:
                if sheet_df[column].dtype == "int64":
                    last_integer_column = column

            if last_integer_column is not None:
                # Rename the last integer column
                sheet_df.rename(
                    columns={
                        "Submitted at": "Date",
                        "Dates": "Date",
                        last_integer_column: "Recommandation",
                    },
                    inplace=True,
                )
            else:
                if column_commu in sheet_df.columns:
                    sheet_df.rename(
                        columns={
                            "Submitted at": "Date",
                            "Dates": "Date",
                            column_commu: "Recommandation",
                        },
                        inplace=True,
                    )
                else:
                    raise Exception("Our colleagues messed up the columns :(")
            sheet_df["Produit"] = name
            sheet_df = sheet_df[["Date", "Recommandation", "Produit"]]
            df = pd.concat([df, sheet_df])

        db.pg_store("gip_suivi_nps", df, NPS_CREATE_TABLE_SQL)

    store_gsheet_task = store_gsheets()

    (start >> store_gsheet_task >> end)
