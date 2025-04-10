import os

from dags.common import slack


def get_default_args():
    return {
        "cwd": os.getenv("AIRFLOW_BASE_DIR"),
        "on_failure_callback": slack.task_fail_alert,
    }
