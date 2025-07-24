import pathlib

import pytest


@pytest.fixture(name="dag_bag", scope="session")
def dag_bag_fixture():
    # Import here because Airflow *really* like to put side effect in their module...
    from airflow.models import DagBag

    return DagBag()


def test_dags_can_be_imported(dag_bag):
    assert dag_bag.import_errors == {}


def test_dags_listing(dag_bag):
    dags_folder = pathlib.Path(__file__).parents[1].joinpath("dags")
    assert dags_folder.exists()
    assert dags_folder.is_dir()

    expected_dag_ids = {f.stem for f in dags_folder.glob("*.py") if f.name != "__init__.py"}
    assert set(dag_bag.dags.keys()) == expected_dag_ids


def test_dags_has_tasks(dag_bag):
    for dag in dag_bag.dags.values():
        assert len(dag.tasks) >= 1, dag
