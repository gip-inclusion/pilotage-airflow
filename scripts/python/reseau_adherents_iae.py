"""Legacy script to generate the "reseau_iae_adherents" table from XLSX files.
Nowadays, the SIRETs should be added to the "reseau_iae_adherents" document
in the Pilotage de l'Inclusion online secured folder.

Those will then be scraped by the reseau_iae_adherents DAG.
"""
import glob
import os

import pandas as pd
from sqlalchemy import create_engine


# add std rule here if col has wrong name
std_cols = {"N°SIRET": "SIRET", "Numéro de SIRET": "SIRET", "SIREN ou SIRET": "SIRET"}

# recover xlsx files from repository containing Reseau.xlsx files
repo_reseaux = "path/to/repo"
reseaux_files = glob.glob(repo_reseaux + "*.xlsx")

reseaux_df = {}
for file in reseaux_files:
    # recover reseau name
    reseau_name = os.path.basename(file).replace(".xlsx", "")
    # read and clean xlsx
    reseau_df = pd.read_excel(file)
    reseau_df.rename(columns=std_cols, inplace=True)
    reseau_df.drop_duplicates(subset=["SIRET"], inplace=True)
    reseau_df.replace(to_replace=" ", regex=True, value="", inplace=True)
    reseau_df["Réseau IAE"] = reseau_name
    reseau_df["SIRET"] = reseau_df["SIRET"].astype(str)
    reseau_df["SIRET"].apply(lambda x: str(x).strip())
    reseau_df["SIRET"].replace(to_replace=".0$", regex=True, value="", inplace=True)
    reseaux_df[reseau_name] = reseau_df[["SIRET", "Réseau IAE"]]


# concatenate all dfs
df = pd.concat(reseaux_df.values())

# import data to database
url = "postgresql://" + user + ":" + password + "@" + host + ":" + port + "/" + database
engine = create_engine(url)
df.to_sql("reseau_iae_adherents", con=engine, if_exists="replace", index=False)
