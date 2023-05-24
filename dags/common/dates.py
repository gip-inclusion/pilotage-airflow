from datetime import datetime, timedelta


def start_of_week():
    today = datetime.today()
    return today - timedelta(days=today.weekday())


def start_of_previous_week():
    return start_of_week() - timedelta(weeks=1)


def end_of_previous_week():
    return start_of_week() - timedelta(days=1)
