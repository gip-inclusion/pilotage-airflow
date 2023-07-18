def computing_etp_part(df):
    df["Montant_total_aide"] = (
        df["effectif_mensuel_conventionné"] * df["af_montant_unitaire_annuel_valeur"] / 12 * df["duree_annexe"]
    )
    df["part_conventionnement_cd"] = df["af_mt_cofinance"] / df["af_montant_total_annuel"]
    df["part_conventionnement_cd"] = df["part_conventionnement_cd"].fillna(0)
    df["etp_conventionnes_cd"] = (df["effectif_mensuel_conventionné"] * df["part_conventionnement_cd"]) * (
        df["duree_annexe"] / 12
    )
    # ETP conventionnés Etat
    df["etp_conventionnes_etat"] = (df["effectif_mensuel_conventionné"] * (1 - df["part_conventionnement_cd"])) * (
        df["duree_annexe"] / 12
    )
    # Nombre de brsa pouvant être subventionnés avec le montant cofinancé (par mois)
    df["nb_brsa_cible_mensuel"] = (df["af_mt_cofinance"] / df["duree_annexe"]) / (0.88 * df["montant_rsa"])
    # idem par an
    df["nb_brsa_cible_annuel"] = df["af_mt_cofinance"] / (0.88 * df["montant_rsa"])
    return df


def model(dbt, session):
    df = dbt.ref("stg_etp_conventionnes")
    df = computing_etp_part(df)
    return df
