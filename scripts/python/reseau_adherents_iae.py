import os
import sys

#database connection
from sqlalchemy import create_engine
from flask_sqlalchemy import sqlalchemy

#librairies
import pandas as pd
import datetime

#Opening the files
df_coorace = pd.read_excel("Coorace.xlsx")
df_fei = pd.read_excel("FEI.xlsx")
df_emmaus = pd.read_excel("Emmaus.xlsx")
df_unai = pd.read_excel("Unai.xlsx")

#Renaming and formatting the columns
df_emmaus.rename(columns = {'N°SIRET' : "SIRET"}, inplace=True)
df_fei.rename(columns = {'Numéro de SIRET' : "SIRET"}, inplace=True)
df_unai['SIRET'].replace(to_replace=' ', regex=True, value="",inplace=True)

#Creating the new columns
df_emmaus["Réseau IAE"] = "Emmaus"
df_fei["Réseau IAE"] = "FEI"
df_coorace["Réseau IAE"] = "Coorace"
df_unai["Réseau IAE"] = "Unai"

#dropping the duplicates
df_emmaus.drop_duplicates(subset=['SIRET'],inplace=True)
df_fei.drop_duplicates(subset=['SIRET'],inplace=True)
df_coorace.drop_duplicates(subset=['SIRET'],inplace=True)
df_unai.drop_duplicates(subset=['SIRET'],inplace=True)

#Concatenating
df = pd.concat([df_emmaus[["SIRET", "Réseau IAE"]],df_coorace[["SIRET", "Réseau IAE"]],df_fei[["SIRET", "Réseau IAE"]],df_unai[["SIRET", "Réseau IAE"]]])

#final formatting
df['SIRET'] = df['SIRET'].astype(str)
df["SIRET"].replace(to_replace='.0$', regex=True, value="",inplace=True)
df['SIRET'] = df['SIRET'].apply(lambda x: str(x).strip())

#Importing the data

engine= sqlalchemy.create_engine("connection information")
print(bool(engine)) # <- just to keep track of the process
df.to_sql("reseau_iae_adherents",con=engine, if_exists="replace",index=False)