"""DAG Airflow des modèles GEIQ."""  # necessaire pour que airflow voit le dag

from airflow.sdk import CronTriggerTimetable

from dags.common import dbt


dag = dbt.tag_build_dag(
    dag_id="dbt_geiq",
    schedule=CronTriggerTimetable("20 8 * * *", timezone="Europe/Paris"),
    tag="geiq",
    build_args="--exclude-resource-type seed",
)
