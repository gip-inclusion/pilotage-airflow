import logging

from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import text

from dags.common import db, default_dag_args, slack


logger = logging.getLogger(__name__)

dag_args = default_dag_args()

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


def get_table_id_pair(conn, table_name):
    """
    Return the pair of table ids for one table.

    The first id corresponds to the table in pilotage.
    The second id corresponds to the matching table in autometa.

    If the table is not present in both databases, return None.
    """
    row = conn.execute(
        text("""
            select
                max(id) filter (where db_id = 2) as pilotage_table_id,
                max(id) filter (where db_id = 18) as autometa_table_id
            from metabase_table
            where name = :table_name
                and db_id in (2, 18)
        """),
        {"table_name": table_name},
    ).fetchone()

    if not row or not row.pilotage_table_id or not row.autometa_table_id:
        return None

    return row.pilotage_table_id, row.autometa_table_id


def update_table_description(conn, pilotage_table_id, autometa_table_id):
    """
    Copy one table description from pilotage to the matching autometa table.
    """
    conn.execute(
        text("""
            update metabase_table as autometa_rows
            set description = pilotage_rows.description
            from metabase_table as pilotage_rows
            where
                pilotage_rows.id = :pilotage_table_id
                and autometa_rows.id = :autometa_table_id
        """),
        {
            "pilotage_table_id": pilotage_table_id,
            "autometa_table_id": autometa_table_id,
        },
    )


def update_field_descriptions(conn, pilotage_table_id, autometa_table_id):
    """
    Copy column descriptions from one pilotage table to the matching autometa table.

    Columns are matched by name.
    Null descriptions from pilotage are ignored.
    """
    conn.execute(
        text("""
            update metabase_field as autometa_rows
            set description = pilotage_rows.description
            from metabase_field as pilotage_rows
            where
                pilotage_rows.table_id = :pilotage_table_id
                and autometa_rows.table_id = :autometa_table_id
                and autometa_rows.name = pilotage_rows.name
                and pilotage_rows.description is not null
        """),
        {
            "pilotage_table_id": pilotage_table_id,
            "autometa_table_id": autometa_table_id,
        },
    )


with DAG("populate_autometa_docs", schedule="0 4 * * *", **dag_args) as dag:

    @task
    def update_metabase_descriptions(table_name):
        """
        Copy table and column descriptions from pilotage to autometa for one table.
        """
        with db.DBConnection(db_url_variable="METABASE_DB_URL_SECRET") as pilo_db, pilo_db.engine.begin() as conn:
            table_id_pair = get_table_id_pair(conn, table_name)

            if not table_id_pair:
                logger.warning("No matching table pair found for %s, skipping.", table_name)
                return

            pilotage_table_id, autometa_table_id = table_id_pair

            update_table_description(
                conn=conn,
                pilotage_table_id=pilotage_table_id,
                autometa_table_id=autometa_table_id,
            )

            update_field_descriptions(
                conn=conn,
                pilotage_table_id=pilotage_table_id,
                autometa_table_id=autometa_table_id,
            )

            logger.info(
                "Updated table and field descriptions for %s. pilotage_table_id=%s, autometa_table_id=%s",
                table_name,
                pilotage_table_id,
                autometa_table_id,
            )

    @task
    def copy_doc_tables():
        """
        Export table and column metadata from pilotage_mb to documentation.doc_autometa_tables
        in the autometa db.
        """
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

    update_tasks = update_metabase_descriptions.expand(table_name=TABLES_TO_UPDATE_AND_COPY)

    copy_task = copy_doc_tables.override(trigger_rule=TriggerRule.ALL_DONE)()

    update_tasks >> copy_task >> slack.warning_notifying_task()
