import pandas as pd


def model(dbt, session):
    organisations = dbt.ref("organisations")
    institutions = dbt.source("emplois", "institutions")
    structures = dbt.source("emplois", "structures")

    potentiel_dict = {}

    # organisations
    for orga_type in organisations["type"].unique():
        cur_pot = len(organisations[organisations["type"] == orga_type])
        potentiel_dict["potentiel" + orga_type] = ["organisation", orga_type, cur_pot]

    # institutions
    for inst_type in institutions["type"].unique():
        cur_pot = len(institutions[institutions["type"] == inst_type])
        potentiel_dict["potentiel" + inst_type] = ["institution", inst_type, cur_pot]

    # structures
    structures = structures[structures["active"] == 1]
    for struct_type in structures["type"].unique():
        cur_pot = len(structures[structures["type"] == struct_type])
        potentiel_dict["potentiel" + struct_type] = ["structure", struct_type, cur_pot]

    return pd.DataFrame.from_dict(potentiel_dict, orient="index", columns=["type", "profil", "potentiel"])
