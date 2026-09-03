"""DAG Airflow des modèles GEIQ."""  # necessaire pour que airflow voit le dag

from dags.common import dbt


dag = dbt.tag_build_dag(dag_id="dbt_geiq", schedule="0 8 * * *", tag="geiq")
