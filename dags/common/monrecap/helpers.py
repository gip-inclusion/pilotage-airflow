import csv
import html
import pathlib

import numpy as np
import pandas as pd


VARIABLES_FILE = pathlib.Path(__file__).parent / "monrecap_variables_barometre.csv"


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
    df = pd.read_csv(pub_sheet_url, decimal=",")

    df.columns = df.columns.str.strip().str.replace("\n", "").str.replace("\r", "")

    # rename columns labels
    variables_dict = {variable_info["question"]: variable for variable, variable_info in variables.items()}
    df = df.rename(columns=variables_dict)
    df = df[variables_dict.values()]
    df = df.replace([np.nan, "NaN"], None)

    # df["dateOrder"] = df["dateOrder"].replace([np.nan, "NaN"], None)

    return df


def convert_date_columns(df, date_columns):
    for col in date_columns:
        # Sometimes the monrecap team might add a column starting with "date" that does not contain a date
        # here we handle this case
        try:
            df[col] = pd.to_datetime(df[col])
            print(f"Successfully converted {col}")
        except Exception as e:
            print(f"Error converting {col}: {e}")

    return df
