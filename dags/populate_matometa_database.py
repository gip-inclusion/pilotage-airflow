import hashlib
import hmac
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

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


def get_hmac_secret():
    return Variable.get("MATOMETA_HMAC_SECRET").encode()


def process_chunk(df, table, secret):
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    for col in COLS_TO_ANONYMIZE:
        if col in df.columns:
            df[col] = df[col].map(
                lambda x, s=secret: hmac.new(s, str(x).encode(), hashlib.sha256).hexdigest() if x is not None else None
            )
            logger.info("Anonymized column %s in table %s.", col, table)

    return df


def sync_tables(table_names, src_schema, dest_schema, from_db=None):
    secret = get_hmac_secret()

    with db.DBConnection(db_url_variable="MATOMETA_DB_URL_SECRET", ssh_conn_id="matometa_scalingo_ssh") as dst_db:
        for table in table_names:
            query = f'SELECT * FROM "{src_schema}"."{table}";'

            for i, chunk in enumerate(from_db.query_chunked(query)):
                chunk = process_chunk(chunk, table, secret)
                if len(chunk) > 0:
                    dst_db.to_sql(chunk, table=table, schema=dest_schema, if_exists="replace" if i == 0 else "append")
                    logger.info("Exported %d rows to %s.%s", len(chunk), dest_schema, table)
                else:
                    logger.info("No data found for table %s, skipping.", table)
                    continue


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
