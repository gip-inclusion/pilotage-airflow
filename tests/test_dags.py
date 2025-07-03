import pathlib

from airflow.models import DagBag


def test_dags_can_be_imported():
    assert DagBag().import_errors == {}


def test_dags_listing():
    dags_folder = pathlib.Path(__file__).parents[1].joinpath("dags")
    assert dags_folder.exists()
    assert dags_folder.is_dir()

    expected_dag_ids = {f.stem for f in dags_folder.glob("*.py") if f.name != "__init__.py"}
    assert set(DagBag().dags.keys()) == expected_dag_ids


def test_dags_has_tasks():
    for dag in DagBag().dags.values():
        assert len(dag.tasks) >= 1, dag
