import csv
import html
import pathlib

import pandas as pd


VARIABLES_FILE = pathlib.Path(__file__).parent / "ESAT_variables_questionnaire2025.csv"


def get_variables() -> dict:
    variables = {}

    with VARIABLES_FILE.open() as file:
        reader = csv.DictReader(file, delimiter=";")

        for row in reader:
            variable = row.get("variable", "").strip()
            type = row.get("type", "")
            question = html.unescape(row.get("question", "")).strip()

            if variable:  # ignore empty variable names (structuring lines)
                variables[variable] = {"type": type, "question": question}

        return variables


def get_data_from_sheet(pub_sheet_url, variables) -> pd.DataFrame:
    dtf = pd.read_csv(pub_sheet_url, decimal=",", na_values=["999", ""])

    # normalize columns
    dtf.columns = dtf.columns.str.replace("\n", "").str.strip()
    dtf.columns = dtf.columns.map(html.unescape).str.strip()

    # rename columns labels
    variables_dict = {variable_info["question"]: variable for variable, variable_info in variables.items()}

    # convert Nan values to nulls
    dtf = dtf.astype("object")
    dtf = dtf.where(pd.notnull(dtf), None)
    dtf = dtf.rename(columns=variables_dict)
    dtf = dtf[variables_dict.values()]

    return dtf
