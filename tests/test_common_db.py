import datetime

import pandas as pd
import sqlalchemy

from dags.common import db


def test_date_column_types():
    df = pd.DataFrame(
        {
            "date_col": [datetime.date(2026, 1, 1), None],
            "text_col": ["a", "b"],
            "int_col": [1, 2],
            "timestamp_col": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "empty_col": [None, None],
        }
    )

    types = db.date_column_types(df)

    assert list(types) == ["date_col"]
    assert isinstance(types["date_col"], sqlalchemy.Date)


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
