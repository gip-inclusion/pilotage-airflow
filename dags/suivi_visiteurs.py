import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import dataframes, dates, db, default_dag_args, slack


with DAG(
    "suivi_visiteurs",
    schedule_interval="@weekly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="calculate_visitors")
    def calculate_visitors(**kwargs):
        with db.MetabaseDBCursor() as (_, conn):
            dtf = pd.read_sql_query('SELECT * FROM "suivi_tb_prive_semaine";', conn)

        out_dtf = dataframes.follow_visits(dtf)
        out_dtf.to_sql("suivi_tb_prive_semaine", con=db.connection_engine(), if_exists="replace", index=False)

    calculate_visitors = calculate_visitors()

    (start >> calculate_visitors >> end)
