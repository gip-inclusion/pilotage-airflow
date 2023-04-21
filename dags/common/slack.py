from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils import timezone


# FIXME(vperron): this webhook should absolutely be installed through the entrypoint
# and not declared manually in the interface.
SLACK_CONN_ID = "slack_webhook"


def task_fail_alert(context):
    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    ti = context.get("task_instance")
    return hook.send_text(
        """
    :airflow: :red_circle: Airflow task failed ! *dag*={dag} *task*={task} <{log_url}|online logs>
    """.format(
            dag=ti.dag_id,
            task=ti.task_id,
            log_url=ti.log_url,
        )
    )


def task_success_alert(context):
    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    dr = context.get("dag_run")
    return hook.send_text(
        """
    :airflow: :white_check_mark: Airflow DAG success. *dag*={dag} *duration_seconds*={duration}
    """.format(
            dag=dr.dag_id,
            duration=(timezone.utcnow() - dr.start_date).total_seconds(),
        )
    )


@task
def success_notifying_task(**kwargs):
    task_success_alert(get_current_context())
