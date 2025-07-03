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
    schedule="@daily",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="store_gsheets")
    def store_gsheets(**kwargs):
        import pandas as pd

        with db.MetabaseDBCursor() as (_, conn):
            suivi_satisfaction = pd.read_sql_query(
                'SELECT * FROM "suivi_satisfaction" WHERE suivi_satisfaction."Recommendation"  is not null;', conn
            )
            suivi_satisfaction = suivi_satisfaction.rename(columns={"Recommendation": "Recommandation"})
            suivi_satisfaction["Produit"] = "Pilotage de l'inclusion"
            suivi_satisfaction = suivi_satisfaction[["Date", "Recommandation", "Produit"]]

        for name, pub_sheet_url in Variable.get("NPS_NAME_PUB_SHEET_URL_TUPLES", deserialize_json=True):
            print(f"reading {name=} at {pub_sheet_url=}")
            sheet_data = pd.read_csv(pub_sheet_url)
            nps_column = None

            # Identify the last column that contains an integer
            for column in sheet_data.columns[::-1]:  # Iterate in a inverse order
                if sheet_data[column].dtype == "int64":
                    nps_column = column
                    break
            else:  # explanation https://docs.python.org/3/tutorial/controlflow.html#break-and-continue-statements-and-else-clauses-on-loops
                nps_column = "Quelle est la probabilité que vous recommandiez La"
                nps_column += "communauté de l'inclusion à un collègue, partenaire ou homologue ?"
            if nps_column not in sheet_data.columns:
                raise Exception("Our colleagues messed up the columns :(")

            sheet_data = sheet_data.rename(
                columns={
                    "Submitted at": "Date",
                    "Dates": "Date",
                    nps_column: "Recommandation",
                },
            )
            sheet_data["Produit"] = name
            sheet_data = sheet_data[["Date", "Recommandation", "Produit"]]
            suivi_satisfaction = pd.concat([suivi_satisfaction, sheet_data])

        db.pg_store("gip_suivi_nps", suivi_satisfaction, NPS_CREATE_TABLE_SQL)

    store_gsheet_task = store_gsheets()

    (start >> store_gsheet_task >> end)
