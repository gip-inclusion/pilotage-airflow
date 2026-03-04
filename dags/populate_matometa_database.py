import hashlib
import hmac
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.ssh.hooks import ssh
from furl import furl

from dags.common import db, dbt, default_dag_args, slack


logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

tables_emploi = ["candidats", "prolongations", "organisations"]
tables_asp = ["fluxIAE_Structure_v2"]
tables_monrecap = ["Contacts", "Commandes"]
tables_datalake = ["pdi_base_unique_tous_les_pros"]
tables_dora = ["les_emplois_utilisateurs"]

col_anonymize = ["hash_nir"]

hmac_secret = Variable.get("MATOMETA_HMAC_SECRET").encode()


def export_tables(tables_to_export, src_schema, dest_schema, src_db_url_variable=None, src_ssh_conn_id=None):
    db_url = furl(Variable.get("MATOMETA_DB_URL_SECRET"))

    ssh_hook = ssh.SSHHook(ssh_conn_id="matometa_scalingo_ssh")

    logger.info("Tunnel creation...")
    with ssh_hook.get_tunnel(remote_port=db_url.port, remote_host=db_url.host) as tunnel:
        logger.info("Tunnel created")
        db_url.host = "127.0.0.1"
        db_url.port = tunnel.local_bind_port
        engine = db.create_engine(db_url.url)

        for table in tables_to_export:
            query = f'SELECT * FROM "{src_schema}"."{table}";'
            if src_db_url_variable and src_ssh_conn_id:
                df = db.create_df_from_ssh_tunnel_db(
                    query, db_url_variable=src_db_url_variable, ssh_conn_id=src_ssh_conn_id
                )
            else:
                df = db.create_df_from_db(query)

            if df is None or df.empty:
                logger.info("No data found for table %s, skipping.", table)
                continue

            logger.info("Retrieved %d rows for table %s.", len(df), table)

            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].map(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

            for col in col_anonymize:
                if col in df.columns:
                    df[col] = df[col].map(
                        lambda x, secret=hmac_secret: hmac.new(secret, str(x).encode(), hashlib.sha256).hexdigest()
                        if x is not None
                        else None
                    )
                    logger.info("Anonymized column %s in table %s.", col, table)

            with engine.begin() as conn:
                df.to_sql(
                    name=table,
                    con=conn,
                    schema=dest_schema,
                    if_exists="replace",
                    index=False,
                    chunksize=5000,
                )
            logger.info("Exported %d rows to %s.%s", len(df), dest_schema, table)


with DAG("populate_matometa_db", schedule="@daily", **dag_args) as dag:

    @task
    def export_emplois_tables():
        export_tables(tables_to_export=tables_emploi, src_schema="public", dest_schema="les_emplois")

    @task
    def export_asp_tables():
        export_tables(tables_to_export=tables_asp, src_schema="public", dest_schema="asp")

    @task
    def export_monrecap_tables():
        export_tables(tables_to_export=tables_monrecap, src_schema="monrecap", dest_schema="monrecap")

    @task
    def export_datalake_tables():
        export_tables(
            tables_to_export=tables_datalake,
            src_schema="public",
            dest_schema="datalake",
            src_db_url_variable="DATALAKE_DB_URL_SECRET",
            src_ssh_conn_id="datalake_scalingo_ssh",
        )

    @task
    def export_dora_tables():
        export_tables(
            tables_to_export=tables_dora,
            src_schema="public_les_emplois",
            dest_schema="dora",
            src_db_url_variable="DORA_DB_URL_SECRET",
            src_ssh_conn_id="dora_scalingo_ssh",
        )

    (
        export_emplois_tables()
        >> export_asp_tables()
        >> export_monrecap_tables()
        >> export_datalake_tables()
        >> export_dora_tables()
        >> slack.success_notifying_task()
    )
