import logging

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.sdk import BaseHook, get_current_context, task
from airflow.utils import timezone


logger = logging.getLogger(__name__)

# FIXME(vperron): this webhook should absolutely be installed through the entrypoint
# and not declared manually in the interface.
SLACK_CONN_ID = "slack_webhook"


def _call_webhook(text):
    try:
        BaseHook.get_connection(SLACK_CONN_ID)
    except AirflowNotFoundException:
        logger.info("Connection %s is not configured, slack notifications are noop.", SLACK_CONN_ID)
        return None
    return SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send_text(text)


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
