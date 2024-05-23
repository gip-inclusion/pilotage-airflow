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
            df = pd.read_sql_query(
                'SELECT * FROM "suivi_satisfaction" WHERE suivi_satisfaction."Recommendation"  is not null;', conn
            )
            df.rename(columns={"Recommendation": "Recommandation"}, inplace=True)
            df["Produit"] = "Pilotage de l'inclusion"
            df = df[["Date", "Recommandation", "Produit"]]

        for name, pub_sheet_url in Variable.get("NPS_NAME_PUB_SHEET_URL_TUPLES", deserialize_json=True):
            print(f"reading {name=} at {pub_sheet_url=}")
            sheet_df = pd.read_csv(pub_sheet_url)
            nps_column = None

            # Identify the last column that contains an integer
            for column in sheet_df.columns[::-1]:  # Iterate in a inverse order
                if sheet_df[column].dtype == "int64":
                    nps_column = column
                    break
            else:  # explanation https://docs.python.org/3/tutorial/controlflow.html#break-and-continue-statements-and-else-clauses-on-loops
                nps_column = "Quelle est la probabilité que vous recommandiez La"
                nps_column += "communauté de l'inclusion à un collègue, partenaire ou homologue ?"
            if nps_column not in sheet_df.columns:
                raise Exception("Our colleagues messed up the columns :(")

            sheet_df.rename(
                columns={
                    "Submitted at": "Date",
                    "Dates": "Date",
                    nps_column: "Recommandation",
                },
                inplace=True,
            )
            sheet_df["Produit"] = name
            sheet_df = sheet_df[["Date", "Recommandation", "Produit"]]
            df = pd.concat([df, sheet_df])

        db.pg_store("gip_suivi_nps", df, NPS_CREATE_TABLE_SQL)

    store_gsheet_task = store_gsheets()

    (start >> store_gsheet_task >> end)
