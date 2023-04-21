import json
from unittest import mock

from airflow.models import DagBag


# keep track of all the necessary variables for any DAGs to run.
# this is good to avoid losing the grip on what is necessary to
# our production flows. Any added variable not listed in the dev
# file will make the tests fail.
def get_dag_variables():
    with open("dag-variables.json", "r", encoding="utf-8") as json_file:
        return json.load(json_file)


@mock.patch.dict(
    "os.environ",
    {f"AIRFLOW_VAR_{var}": "dummy" for var in get_dag_variables()},
)
def test_dags_generic(subtests):
    dagbag = DagBag()
    for dag_id in dagbag.dag_ids:
        with subtests.test(msg="DAG generic checks", dag_id=dag_id):
            dag = dagbag.get_dag(dag_id=dag_id)
            assert dagbag.import_errors == {}
            assert dag is not None
            assert len(dag.tasks) >= 1
