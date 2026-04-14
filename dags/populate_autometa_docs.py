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
    # Returns (pilotage_rows_id, autometa_rows_id) pairs for tables present in both databases (db_id 2 and 18).
    rows = conn.execute(
        text("""
            select array_agg(id order by db_id) as ids
            from metabase_table
            where name = any(:names) and db_id in (2, 18)
            group by name
            having count(distinct db_id) > 1
        """),
        {"names": list(TABLES_TO_UPDATE_AND_COPY)},
    ).fetchall()
    return [(row.ids[0], row.ids[1]) for row in rows]


with DAG("populate_autometa_docs", schedule="0 4 * * *", **dag_args) as dag:

    @task
    def update_table_description():
        # Copies table descriptions from pilotage_mb to autometa_mb for tables in TABLES_TO_UPDATE_AND_COPY.
        with db.DBConnection(db_url_variable="METABASE_DB_URL_SECRET") as pilo_db, pilo_db.engine.begin() as conn:
            table_pair_id = get_table_ids(conn)

            if not table_pair_id:
                logger.info("No matching table pairs found, skipping.")
                return

            tables_id = ", ".join(f"({src}, {dest})" for src, dest in table_pair_id)
            conn.execute(
                text(f"""
                    update metabase_table as autometa_rows
                    set description = pilotage_rows.description
                    from metabase_table as pilotage_rows
                    inner join (values {tables_id}) as id_pairs (src_id, dest_id)
                        on pilotage_rows.id = id_pairs.src_id
                    where autometa_rows.id = id_pairs.dest_id
                """)
            )
            logger.info("Updated descriptions for %d tables.", len(table_pair_id))

    @task
    def update_field_description():
        # Copies column descriptions from pilotage_mb to autometa_mb, matching by table and column name.
        with db.DBConnection(db_url_variable="METABASE_DB_URL_SECRET") as pilo_db, pilo_db.engine.begin() as conn:
            table_pair_id = get_table_ids(conn)

            if not table_pair_id:
                logger.info("No matching table pairs found, skipping.")
                return

            tables_id = ", ".join(f"({src}, {dest})" for src, dest in table_pair_id)
            conn.execute(
                text(f"""
                    update metabase_field as autometa_rows
                    set description = pilotage_rows.description
                    from metabase_field as pilotage_rows
                    inner join (values {tables_id}) as id_pairs (src_id, dest_id)
                        on pilotage_rows.table_id = id_pairs.src_id
                    where
                        autometa_rows.table_id = id_pairs.dest_id
                        and autometa_rows.name = pilotage_rows.name
                        and pilotage_rows.description is not null
                """)
            )
            logger.info("Updated field descriptions for %d table pairs.", len(table_pair_id))

    @task
    def copy_doc_tables():
        # Exports table and column metadata from pilotage_mb to documentation.doc_autometa_tables (in the autometa db)
        table_names = ", ".join(f"'{name}'" for name in TABLES_TO_UPDATE_AND_COPY + TABLES_TO_COPY)
        query = f"""
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
            where mb_table.name in ({table_names})
        """
        with (
            db.DBConnection(db_url_variable="METABASE_DB_URL_SECRET") as src_db,
            db.DBConnection(db_url_variable="MATOMETA_DB_URL_SECRET", ssh_conn_id="matometa_scalingo_ssh") as dst_db,
        ):
            for i, chunk in enumerate(src_db.query_chunked(query)):
                dst_db.to_sql(
                    chunk,
                    table="doc_autometa_tables",
                    schema="documentation",
                    if_exists="replace" if i == 0 else "append",
                )
                logger.info("Exported %d rows to documentation.doc_autometa_tables", len(chunk))

    update_table_description() >> update_field_description() >> copy_doc_tables() >> slack.success_notifying_task()
