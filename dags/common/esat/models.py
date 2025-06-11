import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from dags.common import db
from dags.common.esat.helpers import get_variables_types


DB_SCHEMA = "esat"

# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase
EsatBase = declarative_base()


def create_tables():
    db.create_schema(DB_SCHEMA)
    build_esat_model()
    EsatBase.metadata.create_all(db.connection_engine())


def build_esat_model():
    cols = get_variables_types()

    class_dict = {
        "__tablename__": "esat_questionnaire_2025",
        "__table_args__": {"schema": DB_SCHEMA},
        "__repr__": lambda self: f"<EsatTallyAnswers({', '.join(f'{k}={getattr(self, k)}' for k in cols)})>",
        "primary_key_columns": classmethod(lambda cls: [pk.name for pk in cls.__table__.primary_key.columns]),
    }

    class_dict.update(cols)

    esat_model = type("EsatTallyAnswers", (EsatBase,), class_dict)
    return esat_model


def insert_data_to_db(esat_model, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    engine = db.connection_engine()

    with Session(engine) as session:
        stmt = pg_insert(esat_model).values(df.to_dict("records"))
        stmt = stmt.on_conflict_do_update(
            index_elements=esat_model.primary_key_columns(),
            set_={
                column.name: stmt.excluded[column.name]
                for column in stmt.excluded
                if column.name not in esat_model.primary_key_columns()
            },
        )
        session.execute(stmt)
        session.commit()
