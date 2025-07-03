from airflow.decorators import task

from dags.common import db


@task
def create_schema(schema_name):
    db.create_schema(schema_name)


@task
def create_models(models_base):
    models_base.metadata.create_all(db.connection_engine())
