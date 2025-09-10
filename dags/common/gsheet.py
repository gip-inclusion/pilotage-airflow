import csv
import html

import pandas as pd
import sqlalchemy.types
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from dags.common import db


def get_variables(VARIABLES_FILE, delimiter_type) -> dict:
    variables = {}

    with VARIABLES_FILE.open() as file:
        reader = csv.DictReader(file, delimiter=delimiter_type)

        for row in reader:
            variable = row.get("variable", "").strip()
            type = row.get("type", "")
            question = html.unescape(row.get("question", "")).strip()

            if variable:  # ignore empty variable names (structuring lines)
                variables[variable] = {"type": type, "question": question}

        return variables


def get_data_from_sheet(pub_sheet_url, variables) -> pd.DataFrame:
    df = pd.read_csv(pub_sheet_url, decimal=",", na_values=["999", ""])

    # normalize columns
    df.columns = df.columns.str.replace("\n", "").str.strip()
    df.columns = df.columns.map(html.unescape).str.strip()

    # rename columns labels
    variables_dict = {variable_info["question"]: variable for variable, variable_info in variables.items()}

    # convert Nan values to nulls
    df = df.astype("object")
    df = df.where(pd.notnull(df), None)
    df = df.rename(columns=variables_dict)
    df = df[variables_dict.values()]

    return df


def build_data_model(variables, primary_key, tablename, db_schema, classname, db_base):

    var_types = {}

    for variable, variable_info in variables.items():
        variable_type_str = variable_info["type"]
        variable_type = getattr(sqlalchemy.types, variable_type_str, sqlalchemy.types.String)
        if variable == primary_key:
            var_types[variable] = Column(variable_type, primary_key=True)
        else:
            var_types[variable] = Column(variable_type)

    class_dict = {
        "__tablename__": tablename,
        "__table_args__": {"schema": db_schema},
        "__repr__": lambda self: f"<Answers(submissionID={self.submissionID})>",
        "primary_key_columns": classmethod(lambda cls: [pk.name for pk in cls.__table__.primary_key.columns]),
    }

    class_dict.update(var_types)

    data_model = type(classname, (db_base,), class_dict)
    return data_model


def insert_data_to_db(data_model, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    engine = db.connection_engine()

    with Session(engine) as session:
        stmt = pg_insert(data_model).values(df.to_dict("records"))
        stmt = stmt.on_conflict_do_update(
            index_elements=data_model.primary_key_columns(),
            set_={
                column.name: stmt.excluded[column.name]
                for column in stmt.excluded
                if column.name not in data_model.primary_key_columns()
            },
        )
        session.execute(stmt)
        session.commit()
