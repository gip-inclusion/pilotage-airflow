import json
import pathlib

import pytest


@pytest.fixture
def matometa_dag(monkeypatch):
    for var in json.loads(pathlib.Path("dag-variables.json").read_text()):
        monkeypatch.setenv(f"AIRFLOW_VAR_{var}", "dummy")
    from dags import populate_matometa_database

    return populate_matometa_database


def test_index_statements(matometa_dag):
    assert matometa_dag.index_statements("suspensions_pass", "les_emplois") == [
        'CREATE INDEX IF NOT EXISTS "idx_suspensions_pass_id_pass_agrément"'
        ' ON "les_emplois"."suspensions_pass" ("id_pass_agrément")',
    ]


def test_index_statements_unknown_table(matometa_dag):
    assert matometa_dag.index_statements("barometre", "monrecap") == []


def test_index_statements_name_truncation(matometa_dag):
    statements = matometa_dag.index_statements("suivi_realisation_convention_par_structure", "asp")
    for statement in statements:
        name = statement.split('"')[1]
        assert len(name) <= 63


def test_indexes_only_reference_synced_tables(matometa_dag):
    synced = set(
        matometa_dag.TABLES_EMPLOI
        + matometa_dag.TABLES_ASP
        + matometa_dag.TABLES_MONRECAP
        + matometa_dag.TABLES_DI
        + matometa_dag.TABLES_DATALAKE
        + matometa_dag.TABLES_DORA
    )
    assert set(matometa_dag.INDEXES) <= synced
