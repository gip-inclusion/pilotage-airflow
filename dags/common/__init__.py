import pendulum

from dags.common import slack


def default_dag_args():
    return {
        "start_date": pendulum.datetime(2023, 3, 21, tz="UTC"),
        "catchup": False,
        "default_args": {
            "on_failure_callback": slack.task_fail_alert,
        },
        "max_active_runs": 1,
    }
