import hashlib
import hmac
import logging

import sqlalchemy
from airflow.sdk import Variable, task

from dags.common import db


logger = logging.getLogger(__name__)

COLS_TO_ANONYMIZE = ["hash_nir", "hash_numéro_pass_iae"]

# Hashing is not idempotent, so each anonymized row is flagged and skipped on the next run. The flag
# is dropped along with the table whenever the upstream pipeline reloads it, and new rows default to
# false, so freshly loaded data always gets anonymized exactly once.
ANONYMIZED_FLAG = "is_anonymized"

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
    targets = {}
    for table, column in conn.execute(stmt, {"tables": tables, "cols": COLS_TO_ANONYMIZE}):
        targets.setdefault(table, []).append(column)
    return targets


def _anonymize_column(conn, secret, table, column):
    from psycopg2.extras import execute_values

    select = (
        f'SELECT DISTINCT "{column}" FROM "public"."{table}" WHERE "{column}" IS NOT NULL AND NOT "{ANONYMIZED_FLAG}"'
    )
    values = conn.execute(sqlalchemy.text(select)).scalars().all()
    if not values:
        return

    mapping = [(v, hmac.new(secret, str(v).encode(), hashlib.sha256).hexdigest()) for v in values]

    conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS _anonymize_map"))
    conn.execute(sqlalchemy.text("CREATE TEMP TABLE _anonymize_map (old text, new text) ON COMMIT DROP"))
    with conn.connection.cursor() as cur:
        execute_values(cur, "INSERT INTO _anonymize_map (old, new) VALUES %s", mapping, page_size=10_000)
    conn.execute(sqlalchemy.text("CREATE INDEX ON _anonymize_map (old)"))
    result = conn.execute(
        sqlalchemy.text(
            f'UPDATE "public"."{table}" t SET "{column}" = m.new '
            f'FROM _anonymize_map m WHERE t."{column}"::text = m.old AND NOT t."{ANONYMIZED_FLAG}"'
        )
    )
    logger.info("Anonymized %s.%s: %d distinct values, %d rows updated", table, column, len(mapping), result.rowcount)


def _anonymize_table(conn, secret, table, columns):
    conn.execute(
        sqlalchemy.text(
            f'ALTER TABLE "public"."{table}" '
            f'ADD COLUMN IF NOT EXISTS "{ANONYMIZED_FLAG}" boolean NOT NULL DEFAULT false'
        )
    )
    for column in columns:
        _anonymize_column(conn, secret, table, column)

    # Flagged only once every column of the table has been hashed, in the same transaction.
    result = conn.execute(
        sqlalchemy.text(f'UPDATE "public"."{table}" SET "{ANONYMIZED_FLAG}" = true WHERE NOT "{ANONYMIZED_FLAG}"')
    )
    logger.info("Flagged %d row(s) of %s as anonymized", result.rowcount, table)


@task
def anonymize_nir(tables):
    secret = get_hmac_secret()

    with db.DBConnection(db_url_variable="EMPLOIS_DB_URL_SECRET_local") as emplois_db:
        with emplois_db.engine.begin() as conn:
            targets = _columns_to_anonymize(conn, tables)

        logger.info("Anonymizing %s", targets)
        for table, columns in targets.items():
            with emplois_db.engine.begin() as conn:
                _anonymize_table(conn, secret, table, columns)
