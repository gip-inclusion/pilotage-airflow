import logging

from airflow.decorators import task
from airflow.models import Connection
from airflow.operators.python import get_current_context
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils import timezone
from airflow.utils.session import create_session


logger = logging.getLogger(__name__)

# FIXME(vperron): this webhook should absolutely be installed through the entrypoint
# and not declared manually in the interface.
SLACK_CONN_ID = "slack_webhook"


def _call_webhook(text):
    with create_session() as session:
        if session.query(Connection).filter(Connection.conn_id == SLACK_CONN_ID).count():
            return SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send_text(text)
        else:
            logger.info("Connection %s is not configured, slack notifications are noop.", SLACK_CONN_ID)


def task_fail_alert(context):
    ti = context.get("task_instance")
    return _call_webhook(
        f"""
    :airflow: :red_circle: Airflow task failed ! *dag*={ti.dag_id} *task*={ti.task_id} <{ti.log_url}|online logs>
    """
    )


def task_success_alert(context):
    dr = context.get("dag_run")
    duration = (timezone.utcnow() - dr.start_date).total_seconds()
    return _call_webhook(
        f"""
    :airflow: :white_check_mark: Airflow DAG success. *dag*={dr.dag_id} *duration_seconds*={duration}
    """
    )


@task
def success_notifying_task(**kwargs):
    task_success_alert(get_current_context())
