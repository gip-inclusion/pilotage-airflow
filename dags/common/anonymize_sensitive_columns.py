import hashlib
import hmac
import logging

import sqlalchemy
from airflow.decorators import task
from airflow.models import Variable

from dags.common import db


logger = logging.getLogger(__name__)

COLS_TO_ANONYMIZE = ["hash_nir", "hash_numéro_pass_iae"]

# Each table must be anonymized exactly once per reload (hashing is not idempotent), so a DAG only
# anonymizes the tables its own pipeline reloads just before running.
DAILY_TABLES_TO_ANONYMIZE = ["candidats_v0", "candidatures", "pass_agréments"]
WEEKLY_TABLES_TO_ANONYMIZE = ["fluxIAE_Salarie"]


def get_hmac_secret():
    return Variable.get("MATOMETA_HMAC_SECRET").encode()


def _columns_to_anonymize(conn, tables):
    stmt = sqlalchemy.text(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name IN :tables AND column_name IN :cols
        """
    ).bindparams(
        sqlalchemy.bindparam("tables", expanding=True),
        sqlalchemy.bindparam("cols", expanding=True),
    )
    return conn.execute(stmt, {"tables": tables, "cols": COLS_TO_ANONYMIZE}).fetchall()


def _anonymize_column(conn, secret, table, column):
    from psycopg2.extras import execute_values

    select = f'SELECT DISTINCT "{column}" FROM "public"."{table}" WHERE "{column}" IS NOT NULL'
    values = conn.execute(sqlalchemy.text(select)).scalars().all()
    if not values:
        return

    mapping = [(v, hmac.new(secret, str(v).encode(), hashlib.sha256).hexdigest()) for v in values]

    conn.execute(sqlalchemy.text("CREATE TEMP TABLE _anonymize_map (old text, new text) ON COMMIT DROP"))
    with conn.connection.cursor() as cur:
        execute_values(cur, "INSERT INTO _anonymize_map (old, new) VALUES %s", mapping, page_size=10_000)
    result = conn.execute(
        sqlalchemy.text(
            f'UPDATE "public"."{table}" t SET "{column}" = m.new '
            f'FROM _anonymize_map m WHERE t."{column}"::text = m.old'
        )
    )
    logger.info("Anonymized %s.%s: %d distinct values, %d rows updated", table, column, len(mapping), result.rowcount)


@task
def anonymize_nir(tables):
    secret = get_hmac_secret()

    with db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET_local") as emplois_db:
        with emplois_db.engine.begin() as conn:
            targets = _columns_to_anonymize(conn, tables)

        logger.info("Anonymizing %d column(s): %s", len(targets), targets)
        for table, column in targets:
            with emplois_db.engine.begin() as conn:
                _anonymize_column(conn, secret, table, column)
