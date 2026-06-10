import hashlib
import hmac
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from sqlalchemy import text

from dags.common import db, dbt, default_dag_args, slack


logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

TABLES_EMPLOI = [
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
]
TABLES_ASP = [
    "fluxIAE_Structure_v2",
    "suivi_realisation_convention_par_structure",
    "suivi_realisation_convention_mensuelle",
    "suivi_etp_conventionnes_v2",
    "fluxIAE_ContratMission_v2",
    "fluxIAE_Salarie_v2",
]
TABLES_MONRECAP = ["Contacts", "Commandes", "barometre"]
TABLES_DI = ["structures_v1", "services_v1"]
TABLES_DATALAKE = ["pdi_base_unique_tous_les_pros"]
TABLES_DORA = [
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
]

COLS_TO_ANONYMIZE = ["hash_nir", "hash_numéro_pass_iae"]

# Columns frequently used in WHERE/JOIN clauses by autometa's agent (tables are rebuilt
# daily by `if_exists="replace"`, so indexes must be recreated after each load).
INDEXES = {
    "candidats": [("id",), ("département",)],
    "candidatures_echelle_locale": [
        ("id_candidat",),
        ("id_structure",),
        ("id_org_prescripteur",),
        ("date_candidature",),
        ("état",),
        ("département_structure",),
    ],
    "fiches_de_poste_par_candidature": [("id_candidature",), ("id_fiche_de_poste",)],
    "structures": [("id",), ("département",), ("type",), ("siret",)],
    "prolongations": [("id_pass_agrément",)],
    "organisations": [("id",), ("département",), ("type",)],
    "utilisateurs": [("id",), ("type",)],
    "pass_agréments": [("id",), ("id_candidat",), ("id_structure",), ("date_début",), ("date_fin",)],
    "suspensions_pass": [("id_pass_agrément",)],
    "suivi_auto_prescription": [
        ("id_candidat",),
        ("id_structure",),
        ("date_candidature",),
        ("département_structure",),
    ],
    "fluxIAE_Structure_v2": [("structure_id_siae",)],
    "suivi_realisation_convention_par_structure": [("id_annexe_financiere",), ("structure_id_siae",), ("annee_af",)],
    "suivi_realisation_convention_mensuelle": [("id_annexe_financiere",), ("structure_id_siae",), ("annee_af",)],
    "suivi_etp_conventionnes_v2": [("id_annexe_financiere",), ("structure_id_siae",), ("annee_af",)],
    "fluxIAE_ContratMission_v2": [
        ("contrat_id_ctr",),
        ("contrat_id_pph",),
        ("contrat_id_structure",),
        ("contrat_date_embauche",),
    ],
    "fluxIAE_Salarie_v2": [("salarie_id",)],
    "Commandes": [("département",)],
    "structures_v1": [("id",), ("source",), ("siret",), ("code_postal",)],
    "services_v1": [("id",), ("structure_id",), ("source",)],
    "pdi_base_unique_tous_les_pros": [("email",), ("source",), ("departement_structure",)],
    "structures_structure": [("id",), ("department",), ("siret",)],
    "structures_structuremember": [("user_id",), ("structure_id",)],
    "services_service": [("id",), ("structure_id",), ("status",)],
    "services_service_categories": [("service_id",), ("servicecategory_id",)],
    "orientations_orientation": [("id",), ("service_id",), ("prescriber_structure_id",), ("creation_date",)],
    "users_user": [("id",), ("email",)],
    "stats_searchview": [("date",), ("department",), ("user_kind",)],
    "stats_serviceview": [("date",), ("structure_department",), ("user_kind",), ("service_id",), ("structure_id",)],
    "stats_mobilisationevent": [("date",), ("structure_department",), ("user_kind",), ("service_id",)],
    "stats_structureinfosview": [("date",), ("structure_department",), ("structure_id",)],
    "stats_structureview": [("date",), ("structure_department",), ("user_kind",), ("structure_id",)],
}


def get_hmac_secret():
    return Variable.get("MATOMETA_HMAC_SECRET").encode()


def index_statements(table, schema):
    statements = []
    for columns in INDEXES.get(table, ()):
        # PostgreSQL truncates identifiers above 63 bytes; slicing keeps the name stable for IF NOT EXISTS.
        name = f"idx_{table}_{'_'.join(columns)}"[:63]
        cols = ", ".join(f'"{col}"' for col in columns)
        statements.append(f'CREATE INDEX IF NOT EXISTS "{name}" ON "{schema}"."{table}" ({cols})')
    return statements


def create_indexes(dst_db, table, schema):
    with dst_db.engine.begin() as conn:
        for statement in index_statements(table, schema):
            conn.execute(text(statement))
        conn.execute(text(f'ANALYZE "{schema}"."{table}"'))


def sync_tables(table_names, src_schema, dest_schema, from_db=None):
    secret = get_hmac_secret()

    with db.DBConnection(db_url_variable="MATOMETA_DB_URL_SECRET", ssh_conn_id="matometa_scalingo_ssh") as dst_db:
        for table in table_names:
            query = f'SELECT * FROM "{src_schema}"."{table}";'

            loaded = False
            for i, chunk in enumerate(from_db.query_chunked(query)):
                if len(chunk) == 0:
                    logger.error("Empty chunk for table %s at index %d, aborting.", table, i)
                    break

                for col in chunk.columns:
                    if chunk[col].dtype == object:
                        chunk[col] = chunk[col].map(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

                for col in COLS_TO_ANONYMIZE:
                    if col in chunk.columns:
                        chunk[col] = chunk[col].map(
                            lambda x, s=secret: hmac.new(s, str(x).encode(), hashlib.sha256).hexdigest()
                            if x is not None
                            else None
                        )
                        logger.info("Anonymized column %s in table %s.", col, table)

                dst_db.to_sql(chunk, table=table, schema=dest_schema, if_exists="replace" if i == 0 else "append")
                loaded = True
                logger.info("Exported %d rows to %s.%s", len(chunk), dest_schema, table)
                del chunk

            if loaded:
                create_indexes(dst_db, table, dest_schema)
                logger.info("Created indexes and refreshed statistics for %s.%s", dest_schema, table)


with DAG("populate_matometa_db", schedule="@daily", **dag_args) as dag:

    @task
    def export_emplois_tables():
        with db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET") as src_db:
            sync_tables(TABLES_EMPLOI, src_schema="public", dest_schema="les_emplois", from_db=src_db)

    @task
    def export_asp_tables():
        with db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET") as src_db:
            sync_tables(TABLES_ASP, src_schema="public", dest_schema="asp", from_db=src_db)

    @task
    def export_monrecap_tables():
        with db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET") as src_db:
            sync_tables(TABLES_MONRECAP, src_schema="monrecap", dest_schema="monrecap", from_db=src_db)

    @task
    def export_di_tables():
        with db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET") as src_db:
            sync_tables(TABLES_DI, src_schema="data_inclusion", dest_schema="data_inclusion", from_db=src_db)

    @task
    def export_datalake_tables():
        with db.DBConnection(db_url_variable="DATALAKE_DB_URL_SECRET", ssh_conn_id="datalake_scalingo_ssh") as src_db:
            sync_tables(TABLES_DATALAKE, src_schema="public", dest_schema="datalake", from_db=src_db)

    @task
    def export_dora_tables():
        with db.DBConnection(db_url_variable="DORA_DB_URL_SECRET", ssh_conn_id="dora_scalingo_ssh") as src_db:
            sync_tables(TABLES_DORA, src_schema="public", dest_schema="dora", from_db=src_db)

    (
        export_emplois_tables()
        >> export_asp_tables()
        >> export_monrecap_tables()
        >> export_di_tables()
        >> export_datalake_tables()
        >> export_dora_tables()
        >> slack.success_notifying_task()
    )
