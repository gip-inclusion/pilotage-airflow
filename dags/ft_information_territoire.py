import logging
from collections import namedtuple

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import python

from dags.common import db, default_dag_args, slack
from dags.common.errors import ImproperlyConfiguredException


DB_SCHEMA = "france_travail"
FT_API_BASE_URL = "https://api.francetravail.io/partenaire/stats-offres-demandes-emploi/v1"


Territory = namedtuple("Territory", ["type", "code"])


logger = logging.getLogger(__name__)


# NOTE: We recuperate the stats on a quarterly basis. Since we don't know when the stats will be updated API-side
# we cannot reliably schedule this DAG, so we run it regularly.
with DAG(**default_dag_args(), dag_id="ft_information_territoire", schedule_interval="@weekly") as dag:

    @task(task_id="information_territoire_access_token")
    def information_territoire_access_token(**kwargs):
        # Verify credentials.
        client_id = Variable.get("FT_API_CLIENT_ID")
        client_secret = Variable.get("FT_API_CLIENT_SECRET")

        if client_id == "" or client_secret == "":
            raise ImproperlyConfiguredException(
                "Variables FT_API_CLIENT_ID and FT_API_CLIENT_SECRET must be configured"
            )

        # Request access to the API using our credentials and required scope.
        response = requests.post(
            url="https://entreprise.francetravail.fr/connexion/oauth2/access_token",
            params={"realm": "/partenaire"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "api_stats-offres-demandes-emploiv1 offresetdemandesemploi",
            },
        )
        data = response.json()

        # Return token under the form: Bearer xyz
        return f"{data['token_type']} {data['access_token']}"

    @task(task_id="registered_jobseeker_stats_by_territory")
    def registered_jobseeker_stats_by_territory(access_token, **kwargs):
        # We log which quarters have already been accessed by previous executions of this task
        # If this cache is empty, we'll pull everything available from the API
        logged_sessions_by_territory = Variable.get("FT_INFORMATION_TERRITOIRE_PERIOD_LOG", {})

        table_descriptions = {}  # Maps table name to description during import

        def list_territories(access_token, **kwargs):
            """
            Fetch a list of regions to use with the API.
            Preferred to hardcoding the regions/departments because some regions are unavailable.
            """

            def get_territories_for_type(territory_type):
                response = requests.get(
                    url=f"{FT_API_BASE_URL}/referentiel/territoires/{territory_type}",
                    headers={"Accept": "application/json", "Authorization": access_token},
                )
                return [
                    Territory(type=t["codeTypeTerritoire"], code=t["codeTerritoire"])
                    for t in response.json()["territoires"]
                ]

            return get_territories_for_type("REG") + get_territories_for_type("DEP")

        def get_stats_for_territory(territory_type, territory_code, limit_to_most_recent_quarter=True):
            """
            Makes an API request for the given territory, parses and imports the data in SQL.
            :param territory_type: REG or DEP
            :param limit_to_most_recent_quarter: if True, will request only the most recent quarter from the API
            """
            response = requests.post(
                url=f"{FT_API_BASE_URL}/indicateur/stat-demandeurs",
                headers={
                    "Accept": "application/json",
                    "Authorization": access_token,
                    "Content-Type": "application/json",
                },
                json={
                    "codeTypeTerritoire": territory_type,  # REG / DEP
                    "codeTerritoire": territory_code,
                    "codeTypeActivite": "CUMUL",  # CUMUL = All activities
                    "codeActivite": "CUMUL",
                    "codeTypePeriode": "TRIMESTRE",
                    "codeTypeNomenclature": "CATCAND",  # Stats sur le nombre des demandeurs d'emplois
                    "dernierePeriode": limit_to_most_recent_quarter,
                },
            )
            # NOTE: the response JSON is a very large dictionary
            response_data = response.json()
            data_category = response_data["codeFamille"]
            data = response_data["listeValeursParPeriode"] if "listeValeursParPeriode" in response_data else []

            if limit_to_most_recent_quarter:
                if logged_sessions_by_territory[log_key] == data[0]["codePeriode"]:
                    logger.info(f"No new data for territory {log_key}, skipping SQL import.")
                    return

                # New data available! Continue with the import
                logged_sessions_by_territory[log_key] = data[0]["codePeriode"]

            # The response groups characteristic values by nomenclature,
            # e.g. by category of jobseeker.
            # We restructure the data into tables.
            rows_created = 0
            connection = db.connection_engine()

            for table in data:
                # TODO(calum): for protecting against an obscure hypothetical SQL injection,
                # and more likely against backwards-incompatible table renames,
                # pull the table_name from a configuration rather than from the API response directly
                table_name = f"{data_category}_{table['codeNomenclature']}".lower()
                table_description = table["libNomenclature"]
                table_descriptions[table_name] = table_description

                data_for_rows = {
                    key: table[key]
                    for key in [
                        "codeTypeTerritoire",
                        "codeTerritoire",
                        "libTerritoire",
                        "codeTypeActivite",
                        "codeActivite",
                        "libActivite",
                        "codeNomenclature",
                        "libNomenclature",
                        "codeTypePeriode",
                        "codePeriode",
                        "libPeriode",
                    ]
                    if key in table
                }
                data_for_rows["dateMaj"] = pd.to_datetime(table["datMaj"])

                rows = [row | data_for_rows for row in table["listeValeurParCaract"]]

                # The total isn't included in the list of characteristics, so we add it.
                rows.append(
                    {
                        "codeTypeCaract": "CUMUL",  # Our own value, mimicking their style.
                        "codeCaract": "CUMUL",
                        "libCaract": None,
                        "nombre": table["valeurPrincipaleNombre"],
                        "pourcentage": table["valeurSecondairePourcentage"],
                        "masque": False,
                    }
                    | data_for_rows
                )

                df = pd.DataFrame(rows)
                # Set the index as a unique identifier for the information represented by the row.
                df.set_index(
                    [
                        "codeTypeTerritoire",
                        "codeTerritoire",
                        "codeTypeActivite",
                        "codeActivite",
                        "codeNomenclature",
                        "codeTypePeriode",
                        "codePeriode",
                    ],
                    inplace=True,
                )
                # TODO: upsert behaviour on this query, if the dateMaj is newer
                rows_created += df.to_sql(
                    table_name,
                    con=connection,
                    schema=DB_SCHEMA,
                    if_exists="append",
                    index=True,
                )
                logger.info("Imported %r rows into table %s", rows_created, table_name)

            logger.info(
                "Import complete for territory %s (%s). Created %r rows across %r tables",
                territory_code,
                territory_type,
                rows_created,
                len(table_descriptions),
            )

        # Import data from the API for each territory targeted.
        for territory in list_territories(access_token):
            # If a log is present, we make the assumptions that
            # - the DAG has run successfully since the last quarter
            # - the data we have for previous quarters don't need to be updated
            # - the most recently updated quarter is the only one we are missing
            log_key = f"{territory.type}_{territory.code}"
            limit_to_most_recent_quarter = (
                len(logged_sessions_by_territory) > 0 and log_key in logged_sessions_by_territory
            )
            get_stats_for_territory(territory.type, territory.code, limit_to_most_recent_quarter)

        with db.MetabaseDBCursor() as (cursor, conn):
            for table_name, table_description in table_descriptions.items():
                logger.debug("Adding description to %s: %s", table_name, table_description)
                # TODO: proper cleaning of string
                table_description = table_description.replace("'", "’")
                stmt = f"COMMENT ON TABLE {DB_SCHEMA}.{table_name} IS '{table_description}'"
                cursor.execute(stmt)
            conn.commit()
            logger.info("Updated descriptions for all tables")
        logger.info("Import complete. %r tables were imported", len(table_descriptions))

    access_token = information_territoire_access_token().as_setup()

    processed = registered_jobseeker_stats_by_territory(access_token)

    end = slack.success_notifying_task()

    (
        python.PythonOperator(task_id="create_schema", python_callable=db.create_schema, op_args=[DB_SCHEMA])
        >> processed
        >> end
    )
