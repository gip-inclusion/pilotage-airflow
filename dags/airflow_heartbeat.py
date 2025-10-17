from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.jobs.job import Job
from airflow.utils.db import provide_session

from dags.common import default_dag_args


with DAG(
    "scheduler_heartbeat_monitor",
    schedule="0 * * * *",  # Every hour at minute 0
    **default_dag_args(),
) as dag:

    @task
    @provide_session
    def check_scheduler_heartbeat(session=None, **kwargs):
        """
        Check the last heartbeat of the Airflow scheduler.
        Raises an exception if the scheduler hasn't sent a heartbeat recently.
        """
        # Query the most recent scheduler job
        latest_scheduler = (
            session.query(Job).filter(Job.job_type == "SchedulerJob").order_by(Job.latest_heartbeat.desc()).first()
        )

        if not latest_scheduler:
            raise Exception("No scheduler job found in the database!")

        last_heartbeat = latest_scheduler.latest_heartbeat
        current_time = datetime.now(last_heartbeat.tzinfo) if last_heartbeat.tzinfo else datetime.utcnow()
        time_diff = current_time - last_heartbeat

        # Log the heartbeat information
        print(f"Last scheduler heartbeat: {last_heartbeat}")
        print(f"Current time: {current_time}")
        print(f"Time since last heartbeat: {time_diff}")

        # Alert if heartbeat is older than 5 minutes
        heartbeat_threshold = timedelta(minutes=5)

        if time_diff > heartbeat_threshold:
            error_msg = (
                f"ALERT: Scheduler heartbeat is stale! "
                f"Last heartbeat was {time_diff} ago at {last_heartbeat}. "
                f"Threshold is {heartbeat_threshold}."
            )
            raise Exception(error_msg)

        print(f"Scheduler is healthy. Last heartbeat was {time_diff} ago.")
        return {
            "status": "healthy",
            "last_heartbeat": str(last_heartbeat),
            "time_since_heartbeat": str(time_diff),
        }

    check_scheduler_heartbeat()
