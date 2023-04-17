from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

SLACK_CONN_ID = "slack_webhook"


def task_fail_alert(context):
    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    return hook.send_text(
        """
    :airflow: :red_circle: Airflow Task Failed !  *Task*: {task} *Dag*: {dag} *Logs*: <{log_url}|follow>
    """.format(
            task=context.get("task_instance").task_id,
            dag=context.get("task_instance").dag_id,
            log_url=context.get("task_instance").log_url,
        )
    )


def task_success_alert(context):
    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    return hook.send_text(
        """
    :airflow: :white_check_mark: Airflow DAG Success.  *Dag*: {dag}
    """.format(
            dag=context.get("task_instance").dag_id,
        )
    )


@task
def success_notifying_task(**kwargs):
    task_success_alert(get_current_context())
