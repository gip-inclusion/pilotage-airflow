import pandas as pd


# test à ajouter pour cette table : nb de ligne == nb de lignes fluxIAE_contratMission where contrat_type_contrat = 0


def init_dic(curcontrat):
    contrat = {
        "id_ctr_initial": curcontrat["contrat_id_ctr"],
        "date_embauche": curcontrat["contrat_date_embauche"],
        "date_fin_dernier_contrat": curcontrat["contrat_date_fin_contrat"],
        "duree_contrats_cumules": curcontrat["contrat_duree_contrat"],
        "nombre_reconductions": 0,
    }
    return contrat


def model(dbt, session):
    df = dbt.ref("fluxIAE_ContratMission_v2")

    pph_ids = list(set(df["contrat_id_pph"]))

    contrats = pd.DataFrame(
        columns=[
            "id_ctr_initial",
            "date_embauche",
            "date_fin_dernier_contrat",
            "duree_contrats_cumules",
            "nombre_reconductions",
        ]
    )

    # loop over all physical persons of contracts table
    for pph_id in pph_ids:
        # recover all contrats of this pph
        pph_contrats = df[df["contrat_id_pph"] == pph_id]
        pph_contrats_rows = [x[1] for x in pph_contrats.iterrows()]

        # loop over all contracts to cumulate them
        for curcontrat in pph_contrats_rows:
            # if we get a contrat_type_contrat we add last contract
            # and instantiate a new one
            if curcontrat["contrat_type_contrat"] == 0:
                contrat = init_dic(curcontrat)
                contrats = contrats.append(contrat, ignore_index=True)
            else:
                last_row_index = len(contrats) - 1
                contrats.at[last_row_index, "date_fin_dernier_contrat"] = curcontrat["contrat_date_fin_contrat"]
                contrats.at[last_row_index, "duree_contrats_cumules"] += curcontrat["contrat_duree_contrat"]
                contrats.at[last_row_index, "nombre_reconductions"] += 1

    return contrats
