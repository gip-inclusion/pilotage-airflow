from datetime import date

import pytest

from dags.common.anonymize_sensible_data import normalize_sensible_data


@pytest.mark.parametrize(
    "first_name, last_name",
    [
        ("Jean", "Dupont"),
        ("Jéan", "Dùpont"),
        ("Jeaœn!", "Dupont@"),
        ("Je-An", "Du Po_nt"),
        ("Jean2", "Dupont3!"),
    ],
)
def test_normalization(first_name, last_name):
    birth_date = date(1990, 1, 1)
    expected = "jean|dupont|1990-01-01"  # Expected output for all cases
    result = normalize_sensible_data(first_name, last_name, birth_date)
    assert result == expected
