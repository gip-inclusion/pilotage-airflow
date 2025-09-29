from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from dags.common import db


@task
def create_schema(schema_name):
    db.create_schema(schema_name)


@task(trigger_rule=TriggerRule.NONE_FAILED)
def create_models(models_base):
    models_base.metadata.create_all(db.connection_engine())
