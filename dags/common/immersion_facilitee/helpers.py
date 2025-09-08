import datetime
import logging
import re
import time

import httpx
import pandas as pd
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from dags.common import db
from dags.common.anonymize_sensible_data import NormalizationKind, hash_content, normalize_sensible_data
from dags.common.immersion_facilitee.models import Conventions


MAX_API_ITEMS_RETURNED = 100

logger = logging.getLogger(__name__)


def api_client() -> httpx.Client:
    """
    Crée un client HTTP pour l'API Immersion Facilitee
    :return: un client HTTP configuré
    """
    return httpx.Client(
        base_url=Variable.get("API_IMMERSION_FACILITEE_BASE_URL"),
        headers={"authorization": str(Variable.get("API_IMMERSION_FACILITEE_TOKEN"))},
        timeout=httpx.Timeout(timeout=5, read=30),
    )


def get_all_items(path: str) -> list[dict]:
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

    data = []

    for date_temp in pd.date_range(start_date_greater, start_date_less_or_equal, freq="D"):
        str_date_g = date_temp
        str_date_l = date_temp + relativedelta(days=1)
        response = client.get(
            path,
            params={
                "withStatuses[]": ["ACCEPTED_BY_VALIDATOR"],
                "startDateLessOrEqual": str_date_l.isoformat(),
                "startDateGreater": str_date_g.isoformat(),
            },
        )

        data_partial = response.raise_for_status().json()

        if len(data_partial) >= MAX_API_ITEMS_RETURNED:
            logger.warning(
                "More than %s items found for date %s: %d items. "
                "This may indicate that we do not get all data because of the lack of pagination.",
                MAX_API_ITEMS_RETURNED,
                str_date_g,
                len(data_partial),
            )

        data.extend(data_partial)
        time.sleep(0.21)  # Respecter le rate limit de l'API, 5/s avec une petite marge

    logger.info("Got %r items", len(data))
    return data


def get_dataframe_from_response(table_data: list[dict]) -> pd.DataFrame:
    # list of wanted fiels to keep before and after normalization
    wanted_fields = [
        "id",
        "status",
        "statusJustification",
        "agencyId",
        "dateSubmission",
        "dateStart",
        "dateEnd",
        "dateApproval",
        "dateValidation",
        "siret",
        "immersionObjective",
        "immersionAppellation",
        "immersionAppellationRomeCode",
        "immersionAppellationRomeLabel",
        "immersionAppellationAppellationCode",
        "immersionAppellationAppellationLabel",
        "immersionActivities",
        "immersionSkills",
        "establishmentNumberEmployeesRange",
        "internshipKind",
        "agencyName",
        "agencyDepartment",
        "agencyKind",
        "agencySiret",
        "signatories",
        "beneficiaryPIIHashes",
    ]
    table_data_without_unwanted_fields = [{k: v for k, v in item.items() if k in wanted_fields} for item in table_data]
    df_data = pd.json_normalize(table_data_without_unwanted_fields)
    df_data.columns = [dot_to_camel(col) for col in df_data.columns]
    df_data["beneficiaryPIIHashes"] = df_data.apply(
        lambda row: [
            hash_content(
                normalize_sensible_data(
                    (row["signatoriesBeneficiaryFirstName"], NormalizationKind.NAME),
                    (row["signatoriesBeneficiaryLastName"], NormalizationKind.NAME),
                    (pd.to_datetime(row["signatoriesBeneficiaryBirthdate"]).date(), NormalizationKind.DATE),
                )
            )
        ],
        axis=1,
    )

    wanted_fields = [x for x in wanted_fields if x not in ["signatories", "immersionAppellation"]]

    df_data = df_data[wanted_fields]
    df_data = df_data.where(pd.notna(df_data), None)
    return df_data


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
