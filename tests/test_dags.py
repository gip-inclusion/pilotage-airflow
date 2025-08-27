import json
import pathlib

from airflow.models import DagBag


def test_dags_generic(monkeypatch, subtests):
    # Keep track of all the necessary variables to parse the DAGs.
    for var in json.loads(pathlib.Path("dag-variables.json").read_text()):
        monkeypatch.setenv(f"AIRFLOW_VAR_{var}", "dummy")

    dagbag = DagBag()
    for dag_id in dagbag.dag_ids:
        with subtests.test(msg="DAG generic checks", dag_id=dag_id):
            dag = dagbag.get_dag(dag_id=dag_id)
            assert dagbag.import_errors == {}
            assert dag is not None
            assert len(dag.tasks) >= 1
