
from airflow.models import Variable


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

