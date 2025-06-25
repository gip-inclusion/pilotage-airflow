import csv
import pathlib

import pandas as pd
import sqlalchemy.types
from sqlalchemy import Column


VARIABLES_FILE = pathlib.Path(__file__).parent / "ESAT_variables_questionnaire2025.csv"


def get_variables_dict() -> dict:
    variables_dict = {}

    with VARIABLES_FILE.open() as file:
        reader = csv.DictReader(file, delimiter=";")

        for row in reader:
            variable = row.get("variable", "").strip()
            question = row.get("question", "").replace("&nbsp;", " ").strip()
            if variable and question:
                variables_dict[question] = variable
    return variables_dict


def get_variables_types():
    columns = {}

    with VARIABLES_FILE.open() as file:
        reader = csv.DictReader(file, delimiter=";")

        for row in reader:
            variable = row.get("variable", "").strip()
            variable_type_str = row.get("type", "").strip()

            variable_type = getattr(sqlalchemy.types, variable_type_str, sqlalchemy.types.String)

            if variable:  # ignore empty variable names (structuring lines)
                if variable == "submission_id":
                    columns[variable] = Column(variable_type, primary_key=True)
                else:
                    columns[variable] = Column(variable_type)
        return columns


def get_data_from_sheet(pub_sheet_url, variables_dict) -> pd.DataFrame:
    dtf = pd.read_csv(pub_sheet_url, decimal=",")

    # normalize columns
    dtf.columns = dtf.columns.str.replace("\n", "", regex=True).str.strip()
    dtf.columns = dtf.columns.str.replace("&nbsp;", " ", regex=True).str.strip()

    # rename columns labels
    dtf = dtf.rename(columns=variables_dict)
    dtf = dtf[variables_dict.values()]

    return dtf
