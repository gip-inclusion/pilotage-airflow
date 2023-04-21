from unittest import mock

from dags.common import db


@mock.patch.dict(
    "os.environ",
    {
        "AIRFLOW_VAR_PGHOST": "host",
        "AIRFLOW_VAR_PGPORT": "port",
        "AIRFLOW_VAR_PGDATABASE": "db",
        "AIRFLOW_VAR_PGUSER": "user",
        "AIRFLOW_VAR_PGPASSWORD": "password",
    },
)
def test_connection_envvars():
    envvars = db.connection_envvars()
    assert envvars["PGDATABASE"] == "db"
    assert envvars["PGHOST"] == "host"
    assert envvars["PGPASSWORD"] == "password"
    assert envvars["PGPORT"] == "port"
    assert envvars["PGUSER"] == "user"
