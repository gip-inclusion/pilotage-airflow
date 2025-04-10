import dataclasses
import logging

import httpx
import pandas as pd
from airflow.models import Variable
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from dags.common import db
from dags.common.errors import ImproperlyConfiguredException
from dags.common.france_travail.enums import TerritoryType
from dags.common.france_travail.models import JobSeekerStats


FT_API_AUTH_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
FT_JOBSEEKER_STATS_BASE_URL = "https://api.francetravail.io/partenaire/stats-offres-demandes-emploi/v1"


@dataclasses.dataclass
class Territory:
    type: TerritoryType
    code: str


class MissingPeriodException(Exception):
    pass


logger = logging.getLogger(__name__)


def request_access_token(format_for_header=False):
    # Verify credentials.
    client_id = Variable.get("FT_API_CLIENT_ID")
    client_secret = Variable.get("FT_API_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ImproperlyConfiguredException("Variables FT_API_CLIENT_ID and FT_API_CLIENT_SECRET must be configured")

    # Request access to the API using our credentials and required scope.
    response = httpx.post(
        url=FT_API_AUTH_URL,
        params={"realm": "/partenaire"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "api_stats-offres-demandes-emploiv1 offresetdemandesemploi",
        },
    )
    if response.is_error:
        logger.error(response.json())
    data = response.raise_for_status().json()

    # Return token under the form: Bearer xyz
    return f"{data['token_type']} {data['access_token']}" if format_for_header else data


def list_territories(access_token):
    """
    Fetch a list of territories to use with the API.
    Preferred to hardcoding the territory values because the API does not cover all territories.
    """
    territories = []
    for territory_type in {TerritoryType.Region, TerritoryType.Department}:
        response = httpx.get(
            url=f"{FT_JOBSEEKER_STATS_BASE_URL}/referentiel/territoires/{territory_type}",
            headers={"Accept": "application/json", "Authorization": access_token},
        )
        if response.is_error:
            logger.error(response.json())
        territories += [
            Territory(type=TerritoryType(t["codeTypeTerritoire"]), code=t["codeTerritoire"])
            for t in response.raise_for_status().json()["territoires"]
        ]
    return territories


def get_stats_for_territory(access_token, territory):
    """
    Makes an API request for the given territory, parses and imports the data in SQL.
    :param territory: Territory
    """
    # Table configuration
    # Columns defined in the main body of the request
    shared_columns = [
        "codeTypeTerritoire",
        "codeTerritoire",
        "codePeriode",
        "libTerritoire",
        "codeTypeActivite",
        "codeActivite",
        "libActivite",
        "codeNomenclature",
        "libNomenclature",
        "codeTypePeriode",
        "libPeriode",
        "datMaj",
    ]
    # Columns defined on each characteristic (row) of the table
    characteristic_columns = [
        # Part of the composite primary key
        "codeCaract",
        "codeTypeCaract",
        # Other fields
        "libCaract",
        "nombre",
        "pourcentage",
    ]

    def serialize_table_data_from_response(table_data):
        """Build row data from the main body of the response and data for each characteristic"""

        data_for_rows = {key: table_data.get(key, None) for key in shared_columns}

        rows = [
            {key: row.get(key) for key in characteristic_columns} | data_for_rows
            for row in table_data["listeValeurParCaract"]
        ]

        # The total isn't included in the list of characteristics, so we add it
        rows.append(
            {
                "codeTypeCaract": "CUMUL",  # Our own value, mimicking the API's style
                "codeCaract": "CUMUL",
                "libCaract": None,
                "nombre": table_data["valeurPrincipaleNombre"],
                "pourcentage": table_data["valeurSecondairePourcentage"],
            }
            | data_for_rows
        )

        return rows

    response = httpx.post(
        url=f"{FT_JOBSEEKER_STATS_BASE_URL}/indicateur/stat-demandeurs",
        headers={
            "Accept": "application/json",
            "Authorization": access_token,
        },
        json={
            "codeTypeTerritoire": territory.type,  # REG / DEP
            "codeTerritoire": territory.code,
            "codeTypeActivite": "CUMUL",  # CUMUL = All activities
            "codeActivite": "CUMUL",
            "codeTypePeriode": "TRIMESTRE",
            "codeTypeNomenclature": "CATCAND",  # Stats sur le nombre des demandeurs d'emplois
            "dernierePeriode": False,
        },
    )
    if response.is_error:
        logger.error(response.json())
    # NOTE: the response JSON is a very large dictionary
    response_data = response.raise_for_status().json()
    data = response_data.get("listeValeursParPeriode", [])

    if not data:
        logger.info("No data found for territory %s, skipping SQL import.", territory)
        return

    # The response groups characteristic values by nomenclature,
    # e.g. by category of jobseeker.
    engine = db.connection_engine()

    for nomenclature in data:
        with Session(engine) as session:
            stmt = pg_insert(JobSeekerStats).values(
                pd.DataFrame(serialize_table_data_from_response(nomenclature)).to_dict("records")
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=JobSeekerStats.primary_key_columns(),
                set_={
                    column.name: stmt.excluded[column.name]
                    for column in stmt.excluded
                    if column.name not in JobSeekerStats.primary_key_columns()
                },
            )
            session.execute(stmt)
            session.commit()

    logger.info("Import complete for territory %s.", territory)
