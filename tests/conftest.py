import pytest


@pytest.fixture(scope="session", autouse=True)
def faker_session_locale():
    return [
        "fr_FR",
        "en_US",
        "de_DE",
        "es_ES",
        "it_IT",
        "pl_PL",
        "pt_BR",
        "ar_EG",
    ]
