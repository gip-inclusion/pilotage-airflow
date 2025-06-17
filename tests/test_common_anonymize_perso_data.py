from datetime import date

import pytest
from faker import Faker

from dags.common.anonymize_sensible_data import normalize_sensible_data


@pytest.mark.parametrize(
    "first_name, last_name, birth_date, expected",
    [
        ("Jean", "Dupont", date(1990, 1, 1), "jean|dupont|1990-01-01"),
        ("Jéan", "Dùpont", date(1990, 1, 1), "jean|dupont|1990-01-01"),
        ("Jeaœn!", "Dupont@", date(1990, 1, 1), "jean|dupont|1990-01-01"),
        ("Je-An", "Du Po_nt", date(1990, 1, 1), "jean|dupont|1990-01-01"),
        ("Jean2", "Dupont3!", date(1990, 1, 1), "jean|dupont|1990-01-01"),
        ("Élise", "L'Écuyer", date(1985, 5, 20), "elise|lecuyer|1985-05-20"),
        ("M@rc", "O'Neil", date(1970, 12, 31), "mrc|oneil|1970-12-31"),
        ("Zoë", "D'Alembert", date(2000, 7, 15), "zoe|dalembert|2000-07-15"),
        ("François", "Dupont-Smith", date(1995, 3, 10), "francois|dupontsmith|1995-03-10"),
        ("Anne-Marie", "Le Bris", date(1960, 1, 1), "annemarie|lebris|1960-01-01"),
        ("Éric", "Löwe", date(1980, 8, 8), "eric|lowe|1980-08-08"),
        ("Chloé", "D'Estaing", date(1992, 2, 29), "chloe|destaing|1992-02-29"),
        ("J@ck", "Smith!", date(1975, 11, 5), "jck|smith|1975-11-05"),
        ("Marie-Claire", "Le Roux", date(2001, 4, 17), "marieclaire|leroux|2001-04-17"),
        ("Léo", "O'Brian", date(1988, 9, 9), "leo|obrian|1988-09-09"),
    ],
)
def test_normalize_sensible_data_static(first_name, last_name, birth_date, expected):
    result = normalize_sensible_data(first_name, last_name, birth_date)
    assert result == expected


@pytest.mark.parametrize(
    "locale",
    [
        "fr_FR",
        "en_US",
        "de_DE",
        "es_ES",
        "it_IT",
        "pl_PL",
        "pt_BR",
        "ar_EG",
    ],
)
@pytest.mark.parametrize("i", range(100))
def test_normalize_sensible_data(locale, i):
    fake = Faker(locale)
    first_name = fake.name()
    last_name = fake.address()
    birth_date = fake.date_of_birth()
    result = normalize_sensible_data(first_name, last_name, birth_date)
    parts = result.split("|")
    assert len(parts) == 3
    assert parts[0] == parts[0].lower()
    assert parts[1] == parts[1].lower()
    assert parts[2] == birth_date.strftime("%Y-%m-%d")

    assert parts[0].isalpha()
    assert parts[1].isalpha()


@pytest.mark.parametrize("i", range(100))
def test_normalization_with_faker(i):
    fake = Faker("fr_FR")
    first_name = fake.first_name()
    last_name = fake.last_name()
    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=90)
    result = normalize_sensible_data(first_name, last_name, birth_date)

    parts = result.split("|")
    assert len(parts) == 3
    assert parts[0] == parts[0].lower()
    assert parts[1] == parts[1].lower()
    assert parts[2] == birth_date.strftime("%Y-%m-%d")
