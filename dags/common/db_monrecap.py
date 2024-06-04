from airflow.models import Variable
from sqlalchemy import create_engine


def connection_engine_monrecap():
    database = Variable.get("PGDATABASE_C6")
    host = Variable.get("PGHOST_C6")
    password = Variable.get("PGPASSWORD_C6")
    port = Variable.get("PGPORT_C6")
    user = Variable.get("PGUSER_C6")
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)
