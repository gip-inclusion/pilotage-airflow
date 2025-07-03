import pytest


@pytest.fixture(autouse=True)
def configure(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_TARGET_PATH", tmp_path.joinpath("dbt/target").as_posix())
    monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", "dags")
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")


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
