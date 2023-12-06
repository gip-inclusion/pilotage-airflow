import numpy as np
import pandas as pd


# FTP connection parameters


# Path to the remote file


# Use the "with" statement to ensure the host automatically closes when done
with ftputil.FTPHost(host_name, user_name, password) as host:
    # Check if the remote file exists
    if host.path.isfile(remote_file_path):
        # Download the file to a local temporary file
        with host.open(remote_file_path, "rb") as remote_file:
            df = pd.read_csv(remote_file)
            print(df)
    else:
        print(f"Remote file {remote_file_path} does not exist.")


# Only top the proper France Travail territories
df["C_CODEPOSTAL"] = df["C_CODEPOSTAL"].astype(str).apply(lambda x: "0" + x if len(x) == 4 else x)
df["C_CODEPOSTAL"] = df["C_CODEPOSTAL"].astype(str).str[:2]
values_to_keep = [
    "23",
    "12",
    "27",
    "13",
    "45",
    "64",
    "44",
    "02",
    "21",
    "69",
    "35",
    "53",
    "88",
    "89",
    "78",
    "59",
    "80",
    "97",
]
df = df[df["C_CODEPOSTAL"].isin(values_to_keep)]
rename_values_departement = {
    "23": "Creuse : Département",
    "12": "Aveyron : TAS Villefranche de Rouergue - Decazeville",
    "13": "Bouches-du-Rhône : 5ème et 7ème arrondissements de Marseille",
    "02": "Aisne : Bassin de Laon ",
    "27": "Eure : Louviers et Gisors",
    "21": "Côte d'Or : Beaune et Genlis",
    "35": "Ille-et-Vilaine : Redons-Vallons",
    "44": "Loire-Atlantique : Saint Nazaire, EDS Gare",
    "45": "Loiret : Montargis et rives du Loing",
    "53": "Mayenne : Zone Laval Ouest",
    "64": "Pyrénées-Atlantique : Pau Ouest (Jurançon/ Bilières)",
    "69": "Métropole de Lyon : Givors / Grigny",
    "78": "Yvelines : Saint Quentin en Yvelines",
    "88": "Vosges : Epinal",
    "89": "Yonne : Avallon Tonnerre",
    "59": "Nord : Tourcoing",
    "80": "Somme : Pays du Coquelicot et de la Haute Somme ",
    "97": "La Réunion : St Leu/Trois Bassins",
}
df["C_CODEPOSTAL"] = df["C_CODEPOSTAL"].replace(rename_values_departement)
# Create and/or rename columns, rename values, and change column dtypes
df = df.rename(
    columns={
        "C_CODEPOSTAL": "DEPARTEMENT",
        "C_INDICATEUR": "INDICATEUR ENTREE/SORTIE/STOCK",
        "C_NOMBRESENFANTACHARGE": "ENFANTSACHARGE",
    }
)
df["D_DATENAISSANCE"] = pd.to_datetime(df["D_DATENAISSANCE"])
df["T_DATEEXTRACTION"] = pd.to_datetime(df["T_DATEEXTRACTION"])
df["D_JOUREVENEMENT"] = pd.to_datetime(df["D_JOUREVENEMENT"])
df["D_ENTREEPARCOURS"] = pd.to_datetime(df["D_ENTREEPARCOURS"], format="%Y-%m", errors="coerce")
df["AGE"] = ((df["T_DATEEXTRACTION"] - df["D_DATENAISSANCE"]) / np.timedelta64(1, "Y")).astype(int)
df["D_JOUREVENEMENT"] = pd.to_datetime(df["D_JOUREVENEMENT"], format="%d/%m/%Y")
df = df.rename(columns={"D_JOUREVENEMENT": "SEMAINE"})
change_values_parcours = {"PED": "1. Pro", "PSP": "2. Socio-Pro", "PSO": "3. Social"}
df["C_TYPEPARCOURS"] = df["C_TYPEPARCOURS"].replace(change_values_parcours)
change_values_sexe = {"F": "Femme", "M": "Homme"}
df["C_SEXE"] = df["C_SEXE"].replace(change_values_sexe)
change_values_famille = {
    "CEL": "Célibataire",
    "DIV": "Divorcé(e)",
    "MAR": "Marié(e)",
    "YYYY": "Non renseigné",
    "VEU": "Veuf(ve)",
}
df["C_SITUATIONFAMILLE"] = df["C_SITUATIONFAMILLE"].replace(change_values_famille)
print(df)
# Map new values to columns to prepare for displaying on Metabase
df["ENFANTSACHARGE"] = pd.to_numeric(df["ENFANTSACHARGE"], errors="coerce")
binsenfant = [0, 1, 2, 3, float("inf")]
labelsenfant = ["0 enfant", "1 enfant", "2 enfants", "3 enfants et +"]
df["ENFANTS"] = pd.cut(df["ENFANTSACHARGE"], bins=binsenfant, labels=labelsenfant)
df["ENFANTS"] = df["ENFANTS"].astype(str)
df["ENFANTS"].replace("nan", "Non Renseigné", inplace=True)
formation_mapping = {
    "AFS": "1. Aucun diplôme",
    "C12": "1. Aucun diplôme",
    "C3A": "1. Aucun diplôme",
    "CFG": "1. Aucun diplôme",
    "CP4": "1. Aucun diplôme",
    "NV5": "2. BEP ou CAP",
    "NV4": "3. BAC",
    "NV3": "4. BAC +2,+3,+4",
    "NV2": "4. BAC +2,+3,+4",
    "NV1": "5. BAC +5",
    "YYYY": "Non renseigné",
}
df["FORMATION"] = df["C_NIVEAUFORMATION"].map(formation_mapping)

binsage = [0, 30, 40, 50, float("inf")]
labelsage = ["1. - de 30", "2. 30-40", "3. 40-50", "4. 50+"]
df["tranche_d_age"] = pd.cut(df["AGE"], bins=binsage, labels=labelsage)

freins_cols = [
    "FREIN_ENTREE_FAMILLE",
    "FREIN_ENTREE_ADMIN",
    "FREIN_ENTREE_FINANCE",
    "FREIN_ENTREE_LOGEMENT",
    "FREIN_ENTREE_LANGUESAVOIR",
    "FREIN_ENTREE_MOBILITE",
    "FREIN_ENTREE_NUMERIQUE",
    "FREIN_ENTREE_SANTE",
    "FREIN_ENTREE_FORMATION",
]
for col in freins_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")


def determine_freins(row):
    s = row[freins_cols].sum()
    if s == 0:
        return "0 freins ou freins non renseignés"
    elif s == 1:
        return "1 frein"
    else:
        return "2 freins et +"


df["Freins"] = df.apply(determine_freins, axis=1)
print(df)
