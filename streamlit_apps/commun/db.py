import os

import pandas
from sqlalchemy import create_engine


class MetabaseDatabaseConnection:
    def __init__(self):
        self.engine = None
        self.connection = None

    def __enter__(self):
        try:
            host = os.getenv("PROD_PGHOST")
            port = os.getenv("PROD_PGPORT")
            dbname = os.getenv("PROD_PGDATABASE")
            user = os.getenv("PROD_PGUSER")
            password = os.getenv("PROD_PGPASSWORD")
            db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

            self.engine = create_engine(db_url)
            self.connection = self.engine.connect()
            print("Successfully connected to the database.")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            raise

        return self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.connection:
            self.connection.close()
        print("Database connection closed.")


def create_df_from_db(sql_query: str, params=None):
    with MetabaseDatabaseConnection() as conn:
        return pandas.read_sql_query(sql_query, conn, params=params)
