import pandas as pd
import sqlalchemy.types
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from dags.common import db


DB_SCHEMA = "monrecap"

# NOTE: when upgrading to sqlalchemy 2.0 or higher, we'll need to use the class DeclarativeBase

MonRecap = declarative_base()


def create_tables(variables):
    db.create_schema(DB_SCHEMA)
    build_monrecap_baro_model(variables)
    MonRecap.metadata.create_all(db.connection_engine())


def build_monrecap_baro_model(variables):

    var_types = {}

    for variable, variable_info in variables.items():
        variable_type_str = variable_info["type"]
        variable_type = getattr(sqlalchemy.types, variable_type_str, sqlalchemy.types.String)
        if variable == "submissionid":
            var_types[variable] = Column(variable_type, primary_key=True)
        else:
            var_types[variable] = Column(variable_type)

    class_dict = {
        "__tablename__": "raw_barometre",
        "__table_args__": {"schema": DB_SCHEMA},
        "primary_key_columns": classmethod(lambda cls: [pk.name for pk in cls.__table__.primary_key.columns]),
    }

    class_dict.update(var_types)

    barometre = type("MonRecapBaro", (MonRecap,), class_dict)
    return barometre


def insert_data_to_db(barometre, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    engine = db.connection_engine()

    with Session(engine) as session:
        stmt = pg_insert(barometre).values(df.to_dict("records"))
        stmt = stmt.on_conflict_do_update(
            index_elements=barometre.primary_key_columns(),
            set_={
                column.name: stmt.excluded[column.name]
                for column in stmt.excluded
                if column.name not in barometre.primary_key_columns()
            },
        )
        session.execute(stmt)
        session.commit()
