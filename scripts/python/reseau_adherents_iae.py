import os
import sys
import glob

#database connection
from sqlalchemy import create_engine
#from flask_sqlalchemy import sqlalchemy

#librairies
import pandas as pd
import datetime

# add std rule here if col has wrong name
std_cols = {'N°SIRET': "SIRET",
            'Numéro de SIRET': "SIRET",
            "SIREN ou SIRET": "SIRET"}

# recover xlsx files from repository containing Reseau.xlsx files
repo_reseaux = "/Users/lhuber/Documents/Travail 👩‍💻/GIP🇫🇷/PILOTAGE 🏎/data/réseaux/"
reseaux_files = glob.glob(repo_reseaux+"*.xlsx")

reseaux_df = {}
for file in reseaux_files:
    # recover reseau name
    reseau_name = os.path.basename(file).replace(".xlsx", "")
    # read and clean xlsx
    reseau_df = pd.read_excel(file)
    reseau_df.rename(columns=std_cols, inplace=True)
    reseau_df.drop_duplicates(subset=['SIRET'], inplace=True)
    reseau_df.replace(to_replace=' ', regex=True, value="", inplace=True)
    reseau_df["Réseau IAE"] = reseau_name
    reseau_df['SIRET'] = reseau_df['SIRET'].astype(str)
    reseau_df['SIRET'].apply(lambda x: str(x).strip())
    reseau_df["SIRET"].replace(to_replace='.0$', regex=True, value="",inplace=True)
    reseaux_df[reseau_name] = reseau_df[["SIRET", "Réseau IAE"]]


# concatenate all dfs
df = pd.concat(reseaux_df.values())

# import data to database
engine= sqlalchemy.create_engine("connection information")
print(bool(engine)) # <- just to keep track of the process
df.to_sql("reseau_iae_adherents",con=engine, if_exists="replace",index=False)
