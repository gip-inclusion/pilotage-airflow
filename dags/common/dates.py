import datetime
from datetime import timedelta


def start_of_week():
    today = datetime.date.today()
    return today - timedelta(days=today.weekday())


def start_of_previous_week():
    return start_of_week() - timedelta(weeks=1)


def end_of_previous_week():
    return start_of_week() - timedelta(days=1)


def to_date(value):
    # pandas.to_datetime(format="ISO8601") doesn't work for some value (ie. "0002-08-06")
    return datetime.date.fromisoformat(value) if value else None
