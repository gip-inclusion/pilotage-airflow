import logging

from airflow import DAG
from airflow.decorators import task

from dags.common import db, dbt, default_dag_args, slack


logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


with DAG("populate_esat_database", schedule="@daily", **dag_args) as dag:

    @task
    def export_esat_table():
        query = 'SELECT * FROM "public"."surveys_esatanswer";'
        with (
            db.DBConnection(db_url_variable="PILO_FRONT_DB_URL_SECRET") as src_db,
            db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET") as dst_db,
        ):
            first_write = True
            for chunk in src_db.query_chunked(query):
                dst_db.to_sql(
                    chunk,
                    table="surveys_esatanswer",
                    schema="esat",
                    if_exists="replace" if first_write else "append",
                )
                first_write = False
                logger.info("Exported %d rows to esat.surveys_esatanswer", len(chunk))

    export_esat_table() >> slack.success_notifying_task()
