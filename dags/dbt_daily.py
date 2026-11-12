"""DAG Airflow des modèles daily."""  # necessaire pour que airflow voit le dag

from airflow.sdk import CronTriggerTimetable

from dags.common import dbt


dag = dbt.tag_build_dag(
    dag_id="dbt_daily",
    schedule=CronTriggerTimetable("0 3 * * *", timezone="Europe/Paris"),
    tag="daily",
    build_args="--exclude-resource-type seed --indirect-selection cautious",
)
