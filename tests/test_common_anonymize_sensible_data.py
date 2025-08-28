from datetime import date, datetime

import pytest

from dags.common.anonymize_sensible_data import NormalizationKind, hash_content, normalize_sensible_data


@pytest.mark.parametrize(
    "value, expected",
    [
        ("XXXXX2012369", "7cc9da292b108e91aa40f7287b990daeca22b296e68ee5e0457a89c97a282c27"),
        ("", ""),
        (None, ""),
    ],
)
def test_hash_content(monkeypatch, value, expected):
    monkeypatch.setenv("HASH_SALT", "foobar2000")

    assert hash_content(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("Jean", "jean"),
        ("Léo", "leo"),
        ("Élise", "elise"),
        ("Zoë", "zoe"),
        ("François", "francois"),
        ("Löwe", "lowe"),
        ("letón", "leton"),
        ("Œdipe", "oedipe"),
        ("Anne-Marie", "annemarie"),
        ("Jean2", "jean"),
        ("Je@n", "jen"),
        ("O'Brian", "obrian"),
        ("L'Écuyer", "lecuyer"),
        ("Le Roux", "leroux"),
        ("D.u Po_nt", "dupont"),
        ("Анна", "anna"),
        ("Άννα", "anna"),
    ],
)
def test_normalize_sensible_data_for_name(value, expected):
    assert normalize_sensible_data((value, NormalizationKind.NAME)) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (date(1975, 11, 5), "1975-11-05"),
        (datetime(1985, 5, 20), "1985-05-20"),
        (date(1990, 1, 1), "1990-01-01"),
        (datetime(2001, 4, 17), "2001-04-17"),
    ],
    ids=str,
)
def test_normalize_sensible_data_for_date(value, expected):
    assert normalize_sensible_data((value, NormalizationKind.DATE)) == expected


def test_normalize_sensible_data_with_multiple_datum():
    assert (
        normalize_sensible_data(
            ("Jean", NormalizationKind.NAME),
            ("Dupont", NormalizationKind.NAME),
            (date(1990, 1, 1), NormalizationKind.DATE),
        )
        == "jean|dupont|1990-01-01"
    )


def test_normalize_sensible_data_with_invalid_normalization_kind(faker):
    with pytest.raises(ValueError, match=r"^Unknown normalization kind: .*"):
        normalize_sensible_data((faker.language_name(), faker.pystr()))
