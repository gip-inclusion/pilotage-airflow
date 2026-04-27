import logging

from airflow import DAG
from airflow.decorators import task
from sqlalchemy import text

from dags.common import db, dbt, default_dag_args, slack


logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

TABLES_TO_UPDATE_AND_COPY = (
    "candidats",
    "candidatures_echelle_locale",
    "fiches_de_poste_par_candidature",
    "structures",
    "prolongations",
    "organisations",
    "utilisateurs",
    "pass_agréments",
    "suspensions_pass",
    "suivi_auto_prescription",
    "fluxIAE_Structure_v2",
    "suivi_realisation_convention_par_structure",
    "suivi_realisation_convention_mensuelle",
    "suivi_etp_conventionnes_v2",
    "fluxIAE_ContratMission_v2",
    "fluxIAE_Salarie_v2",
    "Contacts",
    "Commandes",
    "barometre",
    "structures_v1",
    "services_v1",
)

TABLES_TO_COPY = (
    "pdi_base_unique_tous_les_pros",
    "structures_structure",
    "structures_structuremember",
    "services_service",
    "services_servicecategory",
    "services_service_categories",
    "orientations_orientation",
    "users_user",
    "stats_searchview",
    "stats_serviceview",
    "stats_mobilisationevent",
    "stats_structureinfosview",
    "stats_structureview",
)


def get_table_ids(conn):
    rows = conn.execute(
        text("""
            SELECT array_agg(id ORDER BY db_id) AS ids
            FROM metabase_table
            WHERE name = ANY(:names) AND db_id IN (2, 18)
            GROUP BY name
            HAVING COUNT(DISTINCT db_id) > 1
        """),
        {"names": list(TABLES_TO_UPDATE_AND_COPY)},
    ).fetchall()
    return [(row.ids[0], row.ids[1]) for row in rows]


with DAG("populate_autometa_docs", schedule="0 4 * * *", **dag_args) as dag:

    @task
    def update_table_description():
        with db.DBConnection(db_url_variable="METABASE_DB_URL_SECRET") as pilo_db, pilo_db.engine.begin() as conn:
            table_pair_id = get_table_ids(conn)

            if not table_pair_id:
                logger.info("No matching table pairs found, skipping.")
                return

            tables_id = ", ".join(f"({src}, {dest})" for src, dest in table_pair_id)
            conn.execute(
                text(f"""
                    UPDATE metabase_table t1
                    SET description = t2.description
                    FROM metabase_table t2
                    JOIN (VALUES {tables_id}) AS mapping(src_id, dest_id)
                        ON t2.id = mapping.src_id
                    WHERE t1.id = mapping.dest_id
                """)
            )
            logger.info("Updated descriptions for %d tables.", len(table_pair_id))

    @task
    def update_field_description():
        with db.DBConnection(db_url_variable="METABASE_DB_URL_SECRET") as pilo_db, pilo_db.engine.begin() as conn:
            table_pair_id = get_table_ids(conn)

            if not table_pair_id:
                logger.info("No matching table pairs found, skipping.")
                return

            tables_id = ", ".join(f"({src}, {dest})" for src, dest in table_pair_id)
            conn.execute(
                text(f"""
                    UPDATE metabase_field f1
                    SET description = f2.description
                    FROM metabase_field f2
                    JOIN (VALUES {tables_id}) AS mapping(src_id, dest_id)
                        ON f2.table_id = mapping.src_id
                    WHERE f1.table_id = mapping.dest_id
                        AND f1.name = f2.name
                        AND f2.description IS NOT NULL
                """)
            )
            logger.info("Updated field descriptions for %d table pairs.", len(table_pair_id))

    @task
    def copy_doc_tables():
        table_names = ", ".join(f"'{name}'" for name in TABLES_TO_UPDATE_AND_COPY + TABLES_TO_COPY)
        query = f"""
            SELECT
                t.id               AS table_id,
                f.id               AS column_id,
                t.name             AS table_name,
                t.description      AS table_description,
                f.name             AS column_name,
                f.description      AS column_description,
                f.effective_type   AS column_type
            FROM metabase_field AS f
            LEFT JOIN metabase_table AS t ON f.table_id = t.id
            WHERE t.name IN ({table_names})
        """
        with (
            db.DBConnection(db_url_variable="METABASE_DB_URL_SECRET") as src_db,
            db.DBConnection(db_url_variable="MATOMETA_DB_URL_SECRET", ssh_conn_id="matometa_scalingo_ssh") as dst_db,
        ):
            first_write = True
            for chunk in src_db.query_chunked(query):
                dst_db.to_sql(
                    chunk,
                    table="doc_autometa_tables",
                    schema="documentation",
                    if_exists="replace" if first_write else "append",
                )
                first_write = False
                logger.info("Exported %d rows to documentation.doc_autometa_tables", len(chunk))

    update_table_description() >> update_field_description() >> copy_doc_tables() >> slack.success_notifying_task()
