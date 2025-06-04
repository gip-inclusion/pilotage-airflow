import datetime
import logging
import re
import time
from typing import Dict, List

import httpx
import pandas as pd
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from dags.common import db
from dags.common.anonymize_sensible_data import hash_content, normalize_sensible_data
from dags.common.immersion_facilitee.models import Conventions


logger = logging.getLogger(__name__)


def api_client() -> httpx.Client:
    """
    Crée un client HTTP pour l'API Immersion Facilitee
    :return: un client HTTP configuré
    """
    return httpx.Client(
        base_url=Variable.get("API_IMMERSION_FACILITEE_BASE_URL"),
        headers={"authorization": "{}".format(Variable.get("API_IMMERSION_FACILITEE_TOKEN"))},
        timeout=httpx.Timeout(timeout=5, read=30),
    )


def get_all_items(path: str) -> List[Dict]:
    """
    Récupère toutes les conventions des dernières 5 ans
    :param path: chemin de l'API
    :return: liste de dictionnaires contenant les conventions
    """

    # Aujourd'hui on boucle sur les jours car jusqu'à maintenant pour les SIAE il n'y a pas 100
    # ou plus conventions. L'API IF évoluera probablement permettant la pagination et une date de mise à jour

    start_date_less_or_equal = datetime.date.today()
    start_date_greater = start_date_less_or_equal - relativedelta(years=5)

    client = api_client()

    date_temp = start_date_greater

    data = []
    while date_temp < start_date_less_or_equal:
        str_date_g = date_temp.strftime("%Y-%m-%d")
        str_date_l = (date_temp + relativedelta(days=1)).strftime("%Y-%m-%d")
        response = client.get(
            path,
            params={
                "withStatuses[]": ["ACCEPTED_BY_VALIDATOR"],
                "startDateLessOrEqual": str_date_l,
                "startDateGreater": str_date_g,
            },
        )
        response.raise_for_status()

        if response:
            data_partial = response.json()
        else:
            data_partial = []

        data.extend(data_partial)
        date_temp = date_temp + relativedelta(days=1)
        time.sleep(0.5)

    logger.info("Got %r items", len(data))
    return data


def get_dataframe_from_response(table_data: List[Dict]) -> pd.DataFrame:

    fields_to_remove = [
        "schedule",
        "establishmentTutor",
        "validators",
        "agencyCounsellorEmails",
        "agencyValidatorEmails",
        "businessAdvantages",
        "individualProtection",
        "individualProtectionDescription",
        "sanitaryPrevention",
        "sanitaryPreventionDescription",
        "immersionAddress",
        "businessName",
    ]

    table_cleaned = [{k: v for k, v in item.items() if k not in fields_to_remove} for item in table_data]
    df = pd.json_normalize(table_cleaned)
    df.columns = [dot_to_camel(col) for col in df.columns]
    df["beneficiaryId"] = df.apply(
        lambda row: [
            hash_content(
                normalize_sensible_data(
                    row["signatoriesBeneficiaryFirstName"],
                    row["signatoriesBeneficiaryLastName"],
                    pd.to_datetime(row["signatoriesBeneficiaryBirthdate"]).date().isoformat(),
                )
            )
        ],
        axis=1,
    )

    cols_to_remove = [col for col in df.columns if col.startswith("signatories")]
    df = df.drop(columns=cols_to_remove)
    df = df.where(pd.notnull(df), None)
    return df


def dot_to_camel(name: str) -> str:
    return re.sub(r"\.(\w)", lambda m: m.group(1).upper(), name)


def insert_data_to_db(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    engine = db.connection_engine()
    with Session(engine) as session:
        stmt = pg_insert(Conventions).values(df.to_dict("records"))
        stmt = stmt.on_conflict_do_update(
            index_elements=Conventions.primary_key_columns(),
            set_={
                column.name: stmt.excluded[column.name]
                for column in stmt.excluded
                if column.name not in Conventions.primary_key_columns()
            },
        )
        session.execute(stmt)
        session.commit()
