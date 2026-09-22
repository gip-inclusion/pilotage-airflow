"""DAG Airflow des modèles de tous les pros."""  # necessaire pour que airflow voit le dag

from dags.common import dbt


dag = dbt.tag_build_dag(
    dag_id="dbt_tous_les_pros",
    schedule="0 8 * * *",
    tag="pro_pdi",
    build_args="--indirect-selection cautious",
)
