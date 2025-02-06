import dataclasses
import json
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
    response.raise_for_status()
    data = response.json()

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
        response.raise_for_status()
        territories += [
            Territory(type=TerritoryType(t["codeTypeTerritoire"]), code=t["codeTerritoire"])
            for t in response.json()["territoires"]
        ]
    return territories


def get_stats_for_territory(access_token, territory, get_all_periods=False):
    """
    Makes an API request for the given territory, parses and imports the data in SQL.
    :param territory: Territory
    :param get_all_periods: force the API request to get all periods available. Default is
        to get just the most recent quarter
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

    # We log which quarters have already been accessed by previous executions of this task
    # If this cache is empty, we'll pull everything available from the API
    logged_sessions_by_territory = json.loads(Variable.get("FT_INFORMATION_TERRITOIRE_PERIOD_LOG", "{}"))

    # If a log is present, we make the assumptions that
    # - the DAG has run successfully since the last quarter
    # - the data we have for previous quarters don't need to be updated
    # - the most recently updated quarter is the only one we are missing
    log_key = f"{territory.type}_{territory.code}"
    limit_to_most_recent_quarter = bool(logged_sessions_by_territory and log_key in logged_sessions_by_territory)

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
            "dernierePeriode": limit_to_most_recent_quarter,
        },
    )
    # NOTE: the response JSON is a very large dictionary
    response.raise_for_status()
    response_data = response.json()
    data = response_data.get("listeValeursParPeriode", [])

    if not data:
        logger.info("No data found for territory %s, skipping SQL import.", territory)
        return

    periods_returned = {datum["codePeriode"] for datum in data}
    periods_cached = set(logged_sessions_by_territory.get(log_key, []))
    if limit_to_most_recent_quarter:
        if periods_returned - periods_cached == set():
            logger.info("No new data for territory %s, skipping SQL import.", log_key)
            return

        # New data available! Continue with the import.
        # Use set to remove duplicates, but store as a list for JSON support.
        logged_sessions_by_territory[log_key] = list(periods_cached | periods_returned)
    else:
        logged_sessions_by_territory[log_key] = list(periods_returned)

    # The response groups characteristic values by nomenclature,
    # e.g. by category of jobseeker.
    engine = db.connection_engine()

    for nomenclature in data:
        df = pd.DataFrame(serialize_table_data_from_response(nomenclature))

        with Session(engine) as session:
            stmt = pg_insert(JobSeekerStats).values(df.to_dict("records"))
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

    Variable.set("FT_INFORMATION_TERRITOIRE_PERIOD_LOG", json.dumps(logged_sessions_by_territory))
    logger.info("Import complete for territory %s.", territory)
