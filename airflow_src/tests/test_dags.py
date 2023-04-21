from unittest import mock

from airflow.models import DagBag


# keep track of all the necessary variables for any DAGs to run.
# this is good to avoid losing the grip on what is necessary to
# our production flows. Any added variable not listed here will
# make the tests fail.
ALL_VARIABLES = [
    "NPS_NAME_PUB_SHEET_URL_TUPLES",
    "PGDATABASE",
    "PGHOST",
    "PGPASSWORD",
    "PGPORT",
    "PGUSER",
    "RESEAU_IAE_ADHERENTS_PUB_SHEET_URL",
]


@mock.patch.dict(
    "os.environ",
    {f"AIRFLOW_VAR_{var}": "dummy" for var in ALL_VARIABLES},
)
def test_dags_generic(subtests):
    dagbag = DagBag()
    for dag_id in dagbag.dag_ids:
        with subtests.test(msg="DAG generic checks", dag_id=dag_id):
            dag = dagbag.get_dag(dag_id=dag_id)
            assert dagbag.import_errors == {}
            assert dag is not None
            assert len(dag.tasks) >= 1
