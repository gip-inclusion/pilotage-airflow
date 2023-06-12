from datetime import datetime, timedelta


def get_current_day():
    today = datetime.today()
    start_of_week = today - timedelta(days=today.weekday())
    start_of_previous_week = start_of_week - timedelta(weeks=1)
    end_of_previous_week = start_of_week - timedelta(days=1)
    return start_of_previous_week, end_of_previous_week
