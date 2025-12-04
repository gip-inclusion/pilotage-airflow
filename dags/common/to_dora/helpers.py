import logging

import pandas as pd
from airflow.models import Variable
from sqlalchemy import delete, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from dags.common import db
from dags.common.to_dora.models import Utilisateurs


logger = logging.getLogger(__name__)


def get_les_emplois_users() -> pd.DataFrame:
    query = 'SELECT * FROM "utilisateurs";'
    les_emplois_users = db.create_df_from_db(query)
    logger.info("Retrieved %d lines for Les-Emplois users.", len(les_emplois_users))
    return les_emplois_users


def get_les_emplois_users_subset(data=pd.DataFrame) -> pd.DataFrame:
    cleaned_data = data.dropna(subset=["email"])
    cols_to_keep = [col for col in cleaned_data.columns if col in inspect(Utilisateurs).columns]
    return cleaned_data[cols_to_keep]


def connection_dora_engine():
    database = Variable.get("DORA_PGDATABASE")
    host = Variable.get("DORA_PGHOST")
    password = Variable.get("DORA_PGPASSWORD")
    port = Variable.get("DORA_PGPORT")
    user = Variable.get("DORA_PGUSER")
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return db.create_engine(url)


def insert_data_to_dora_db(data: pd.DataFrame) -> None:
    if data is None or data.empty:
        logger.info("No data to insert into Dora database.")
        return

    engine = connection_dora_engine()

    Utilisateurs.__table__.create(engine, checkfirst=True)

    with Session(engine) as session:
        session.execute(delete(Utilisateurs))
        data_to_store = get_les_emplois_users_subset(data)

        session.execute(pg_insert(Utilisateurs).values(data_to_store.to_dict(orient="records")))
        session.commit()
