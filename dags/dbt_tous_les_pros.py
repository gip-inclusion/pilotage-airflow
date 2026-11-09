"""DAG Airflow des modèles de tous les pros."""  # necessaire pour que airflow voit le dag

from airflow.sdk import CronTriggerTimetable

from dags.common import dbt


dag = dbt.tag_build_dag(
    dag_id="dbt_tous_les_pros",
    schedule=CronTriggerTimetable("40 8 * * *", timezone="Europe/Paris"),
    tag="pro_pdi",
    build_args="--exclude-resource-type seed --indirect-selection cautious",
)
