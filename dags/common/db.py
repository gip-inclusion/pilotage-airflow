import textwrap

import sqlalchemy
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.sql.ddl import CreateSchema

from dags.common import dataframes


def connection_envvars():
    return {
        "PGHOST": Variable.get("PGHOST"),
        "PGPORT": Variable.get("PGPORT"),
        "PGDATABASE": Variable.get("PGDATABASE"),
        "PGUSER": Variable.get("PGUSER"),
        "PGPASSWORD": Variable.get("PGPASSWORD"),
    }


def connection_engine():
    database = Variable.get("PGDATABASE")
    host = Variable.get("PGHOST")
    password = Variable.get("PGPASSWORD")
    port = Variable.get("PGPORT")
    user = Variable.get("PGUSER")
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


class MetabaseDBCursor:
    def __init__(self):
        self.cursor = None
        self.connection = None

    def __enter__(self):
        import psycopg2

        self.connection = psycopg2.connect(
            host=Variable.get("PGHOST"),
            port=Variable.get("PGPORT"),
            dbname=Variable.get("PGDATABASE"),
            user=Variable.get("PGUSER"),
            password=Variable.get("PGPASSWORD"),
        )
        self.cursor = self.connection.cursor()
        return self.cursor, self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


class MetabaseDatabaseCursor3:
    """
    dbt-postgres requires us to use psycopg2 for our database connections in most DAGs.

    We use psycopg3 for database connections where the utility of this library is useful,
    but where dbt is not relevant.

    If and when dbt-postgres moves from psycopg2 this class will replace MetabaseDBCursor
    https://github.com/dbt-labs/dbt-postgres/issues/122
    """

    def __init__(self):
        self.cursor = None
        self.connection = None

    def __enter__(self):
        import psycopg

        self.connection = psycopg.connect(
            host=Variable.get("PGHOST"),
            port=Variable.get("PGPORT"),
            dbname=Variable.get("PGDATABASE"),
            user=Variable.get("PGUSER"),
            password=Variable.get("PGPASSWORD"),
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=5,
            keepalives_count=5,
        )
        self.cursor = self.connection.cursor()
        return self.cursor, self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def pg_store(table_name, df, create_table_sql):
    from psycopg2 import sql

    with MetabaseDBCursor() as (cursor, conn):
        cursor.execute(sql.SQL(textwrap.dedent(create_table_sql)).format(table_name=sql.Identifier(table_name)))
        conn.commit()
        cursor.copy_from(dataframes.to_buffer(df), table_name, sep=",")
        conn.commit()


def create_df_from_db(sql_query):
    import pandas as pd

    with MetabaseDBCursor() as (cursor, conn):
        return pd.read_sql_query(sql_query, conn)


def drop_view(view_name):
    from psycopg2 import sql

    with MetabaseDBCursor() as (cursor, conn):
        drop_view_query = sql.SQL("DROP VIEW IF EXISTS {name};").format(name=sql.Identifier(view_name))
        cursor.execute(drop_view_query)
        conn.commit()


def create_schema(schema_name):
    # TODO: Use an Airflow Connection
    with connection_engine().connect() as connection:
        connection.execute(CreateSchema(schema_name))


@sqlalchemy.event.listens_for(sqlalchemy.Table, "before_create")
def create_schema_if_not_exists(target, connection, **_):
    connection.execute(CreateSchema(target.schema))
