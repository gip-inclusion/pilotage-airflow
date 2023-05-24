from datetime import datetime, timedelta


today = datetime.today()
start_of_week = today - timedelta(days=today.weekday())  # Start of the current week
start_of_previous_week = start_of_week - timedelta(weeks=1)  # Start of the previous week
end_of_previous_week = start_of_week - timedelta(days=1)  # End of the previous week
