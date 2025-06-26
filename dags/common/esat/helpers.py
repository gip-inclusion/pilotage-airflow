import csv
import pathlib

import pandas as pd


VARIABLES_FILE = pathlib.Path(__file__).parent / "ESAT_variables_questionnaire2025.csv"


def get_variables() -> dict:
    variables = {}

    with VARIABLES_FILE.open() as file:
        reader = csv.DictReader(file, delimiter=";")

        for row in reader:
            variable = row.get("variable", "").strip()
            question = row.get("question", "").replace("&nbsp;", " ").strip()

            if variable:  # ignore empty variable names (structuring lines)
                variables[variable] = {"type": variable, "question": question}

        return variables


def get_data_from_sheet(pub_sheet_url, variables_dict) -> pd.DataFrame:
    dtf = pd.read_csv(pub_sheet_url, decimal=",")

    # normalize columns
    dtf.columns = dtf.columns.str.replace("\n", "", regex=True).str.strip()
    dtf.columns = dtf.columns.str.replace("&nbsp;", " ", regex=True).str.strip()

    # rename columns labels
    dtf = dtf.rename(columns=variables_dict)
    dtf = dtf[variables_dict.values()]

    return dtf
