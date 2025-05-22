import hashlib
import json
import logging
import os
from typing import Dict, List
import unicodedata
import re
from dateutil.relativedelta import relativedelta

import httpx
import pandas as pd
from airflow.models import Variable
from dags.common import db
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dags.common.immersion_facilitee.models import Conventions
import re


logger = logging.getLogger(__name__)


def api_client() -> httpx.Client:


    return httpx.Client(
        base_url=Variable.get("API_IMMERSION_FACILITEE_BASE_URL"),
        headers={
            "authorization": "{}".format(Variable.get("API_IMMERSION_FACILITEE_TOKEN"))
            },
        timeout=httpx.Timeout(timeout=5, read=30),
    )

def get_all_items(path: str) -> List[Dict]:

    """
    Récupère toutes les conventions des dernières 5 ans
    :param path: chemin de l'API
    :return: liste de dictionnaires contenant les conventions
    """

    # dans le staging il y a que 20 données
    # dans la prod la limite est 100
    # il faut voir s'ils peuvent faire une pagination
    # je trouve pas pratique de devoir jouer avec les dates. Ou oui si on fait quelques choses d'incrementale

    client = api_client()
    # le filtre ci-dessous est à changer si je pass que un status l'API n'est pas contente!
    response = client.get(path,params={"withStatuses[]":["ACCEPTED_BY_VALIDATOR"]})
    response.raise_for_status()
    data = response.json()
    logger.info("Got %r items", len(data))
    return data

def hash_content(content : str) -> str:
    # duplicate de ce qui est fait dans fluxIAE => vaudrait mieux mutualiser ailleurs ?
    return hashlib.sha256(f'{content}{os.getenv("HASH_SALT")}'.encode()).hexdigest()



def normalize_sensible_data(first_name, last_name, birth_date):
    # je me dis que ça serait bien de la tester cette méthode ?
    content = f"{first_name}{last_name}{birth_date.strftime('%d%m%Y')}"

    normalized_content = unicodedata.normalize("NFKD", content)

    normalized_content = re.sub((r"[^a-zA-Z0-9]+"), "", normalized_content).lower()

    return normalized_content



def get_dataframe_from_response(table_data):

    fields_to_remove = [
    'schedule',
    'establishmentTutor',
    'validators',
    'agencyCounsellorEmails',
    'agencyValidatorEmails',
    'businessAdvantages',
    'individualProtection',
    'individualProtectionDescription',
    'sanitaryPrevention',
    'sanitaryPreventionDescription',
    'immersionAddress',
    'businessName'
    ]


    table_cleaned = [
        {k: v for k, v in item.items() if k not in fields_to_remove}
        for item in table_data
    ]

    df = pd.json_normalize(table_cleaned)
    df.columns = [dot_to_camel(col) for col in df.columns]

    df['beneficiaryId'] = df.apply(
        lambda row: hash_content(normalize_sensible_data(
            row['signatoriesBeneficiaryFirstName'],
            row['signatoriesBeneficiaryLastName'],
            pd.to_datetime(row[ 'signatoriesBeneficiaryBirthdate']).date()
        )),
        axis=1
    )

    cols_to_remove = [ col for col in df.columns if col.startswith("signatories")]
    df = df.drop(columns=cols_to_remove)
    df = df.where(pd.notnull(df), None)
    return df

def dot_to_camel(name):
    return re.sub(r'\.(\w)', lambda m: m.group(1).upper(), name)


def insert_data_to_db(df : pd.DataFrame):
    engine = db.connection_engine()
    with Session(engine) as session :
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