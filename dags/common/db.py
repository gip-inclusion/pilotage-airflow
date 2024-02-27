import textwrap

from airflow.models import Variable
from sqlalchemy import create_engine

from . import dataframes


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
    with MetabaseDBCursor() as (cursor, conn):
        drop_view_query = f"DROP VIEW IF EXISTS {view_name};"
        cursor.execute(drop_view_query)
        conn.commit()
