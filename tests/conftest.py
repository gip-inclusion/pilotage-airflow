import pathlib

import pytest


@pytest.fixture(autouse=True)
def configure(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_TARGET_PATH", tmp_path.joinpath("dbt/target").as_posix())
    monkeypatch.setenv("AIRFLOW_HOME", pathlib.Path(__file__).parent.parent.as_posix())
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch.setenv("SQLALCHEMY_SILENCE_UBER_WARNING", "1")


@pytest.fixture(scope="session", autouse=True)
def faker_session_locale():
    return [
        "fr_FR",
        "en_US",
        "de_DE",
        "es_ES",
        "it_IT",
        "pl_PL",
        "pt_BR",
        "ar_EG",
    ]
