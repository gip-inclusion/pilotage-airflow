"""DAG Airflow des modèles de tous les pros."""  # necessaire pour que airflow voit le dag

from airflow.sdk import CronTriggerTimetable

from dags.common import dbt


dag = dbt.tag_build_dag(
    dag_id="dbt_emplois",
    schedule=CronTriggerTimetable("0 8 * * *", timezone="Europe/Paris"),
    tag="emplois",
    build_args="--exclude-resource-type seed --indirect-selection cautious",
)
