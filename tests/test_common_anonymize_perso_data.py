from datetime import datetime

from dags.common.anonymize__sensible_data import normalize_sensible_data

def test_normal_case():
    first_name = "Jean"
    last_name = "Dupont"
    birth_date = datetime(1990, 1, 1)

    result = normalize_sensible_data(first_name, last_name, birth_date)
    expected = "jeandupont01011990"

    assert result == expected

def test_accents():
    first_name = "Jéan"
    last_name = "Dùpont"
    birth_date = datetime(1990, 1, 1)

    result = normalize_sensible_data(first_name, last_name, birth_date)
    expected = "jeandupont01011990"

    assert result == expected


def test_special_characters():
    first_name = "Jeaœn!"
    last_name = "Dupont@"
    birth_date = datetime(1990, 1, 1)

    result = normalize_sensible_data(first_name, last_name, birth_date)
    expected = "jeandupont01011990"

    assert result == expected

def test_with_spaces_and_dashes():
    first_name = "Jean-Pierre"
    last_name = "Dupont Smith_"
    birth_date = datetime(1990, 1, 1)

    result = normalize_sensible_data(first_name, last_name, birth_date)
    expected = "jeanpierredupontsmith01011990"

    assert result == expected