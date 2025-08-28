from dags.common import db


def test_connection_envvars(monkeypatch):
    expected = {
        "PGHOST": "host",
        "PGPORT": "port",
        "PGDATABASE": "db",
        "PGUSER": "user",
        "PGPASSWORD": "password",
    }
    for name, value in expected.items():
        monkeypatch.setenv(f"AIRFLOW_VAR_{name}", value)

    assert db.connection_envvars() == expected
