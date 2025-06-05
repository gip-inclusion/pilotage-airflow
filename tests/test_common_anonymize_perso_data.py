from datetime import datetime

import pytest

from dags.common.anonymize_sensible_data import normalize_sensible_data


@pytest.mark.parametrize(
    "first_name, last_name",
    [
        ("Jean", "Dupont"),
        ("Jéan", "Dùpont"),
        ("Jeaœn!", "Dupont@"),
        ("Je-An", "Du Po_nt"),
    ],
)
def test_normalization(first_name, last_name):
    birth_date = datetime(1990, 1, 1).isoformat()
    expected = "jean-dupont-19900101t000000"  # Expected output for all cases
    result = normalize_sensible_data(first_name, last_name, birth_date)
    assert result == expected
