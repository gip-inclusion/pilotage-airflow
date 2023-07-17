from datetime import datetime

import pandas  # init pandas to avoid a crash with freezegun  # noqa: F401
from freezegun import freeze_time

from dags.common import dates


@freeze_time("2022-06-08")
def test_date_functions():
    assert dates.start_of_week() == datetime(2022, 6, 6)
    assert dates.start_of_previous_week() == datetime(2022, 5, 30)
    assert dates.end_of_previous_week() == datetime(2022, 6, 5)
    # FIXME(vperron) : this function is VERY weird, it relies internally on a fixed
    # date, 2022-06-06. WHY ?
    assert dates.week_list() == [datetime(2022, 6, 6)]
