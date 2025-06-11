import csv
import os

import pandas as pd
from sqlalchemy import Column
from sqlalchemy.types import Boolean, DateTime, Float, Integer, String


TYPE_MAP = {"Boolean": Boolean, "String": String, "DateTime": DateTime, "Integer": Integer, "Float": Float, "": String}
CURRENT_DIR = os.path.dirname(__file__)
VARIABLES_FILE = os.path.join(CURRENT_DIR, "ESAT_variables_questionnaire2025.csv")


def get_variables_dict() -> dict:
    variables_dict = {}

    with open(VARIABLES_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=";")
        next(reader)  # Sauter l'en-tête

        for row in reader:
            if len(row) >= 2:
                variable = row[0].strip()
                question = row[1].replace("&nbsp;", " ").strip()
                if not (variable == "" or question == ""):
                    variables_dict[question] = variable
    return variables_dict


def get_variables_types():
    columns = {}

    with open(VARIABLES_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=";")
        next(reader)

        for row in reader:
            if len(row) >= 2:
                variable = row[0].strip()
                variable_type = TYPE_MAP[row[2]]

                # define variable types (ignore empty names, they correspond to structuring lines and not to variables)
                if not (variable == "" or variable_type == ""):
                    # submissionID is the primary key
                    if variable == "submissionID":
                        columns[variable] = Column(variable_type, primary_key=True)
                    else:
                        columns[variable] = Column(variable_type)
        return columns


def get_data_from_sheet(pub_sheet_url, variables_dict, variables_types) -> pd.DataFrame:
    dtf = pd.read_csv(pub_sheet_url)

    # normalize columns
    dtf.columns = dtf.columns.str.replace("\n", "", regex=True).str.strip()
    dtf.columns = dtf.columns.str.replace("&nbsp;", " ", regex=True).str.strip()

    # rename columns labels
    dtf = dtf.rename(columns=variables_dict)
    dtf = dtf[variables_dict.values()]

    # normalize floats
    float_columns = [col for col, col_type in variables_types.items() if str(col_type.type) == "FLOAT"]
    for col in float_columns:
        dtf[col] = dtf[col].astype(str).str.replace(",", ".", regex=False).astype(float)

    return dtf
