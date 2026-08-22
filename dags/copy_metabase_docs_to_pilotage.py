import logging

from airflow.sdk import DAG, task
from sqlalchemy import text

from dags.common import db, dbt, default_dag_args, slack


logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


with DAG("copy_metabase_docs_to_pilotage", schedule="0 4 * * *", **dag_args) as dag:

    @task
    def copy_doc_tables():
        # Exports table and column metadata from pilotage_mb to documentation.doc_autometa_tables (in the pilotage db)
        query = """
            select
                mb_table.id             as table_id,
                mb_field.id             as column_id,
                mb_table.name           as table_name,
                mb_table.description    as table_description,
                mb_field.name           as column_name,
                mb_field.description    as column_description,
                mb_field.effective_type as column_type
            from metabase_field as mb_field
            left join metabase_table as mb_table on mb_field.table_id = mb_table.id
            where mb_table.db_id = 2
                and (
                        mb_table.description is not null
                        or mb_field.description is not null
                    )
                and mb_table.name not like 'stg\\_%%'
        """
        with (
            db.DBConnection(db_url_variable="METABASE_DB_URL_SECRET") as src_db,
            db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET") as dst_db,
        ):
            with dst_db.engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS documentation"))

            for i, chunk in enumerate(src_db.query_chunked(query)):
                dst_db.to_sql(
                    chunk,
                    table="doc_autometa_tables",
                    schema="documentation",
                    if_exists="replace" if i == 0 else "append",
                )
                logger.info("Exported %d rows to documentation.doc_autometa_tables", len(chunk))

    copy_doc_tables() >> slack.success_notifying_task()
