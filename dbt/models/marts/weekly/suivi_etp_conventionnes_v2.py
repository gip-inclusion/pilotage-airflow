def model(dbt, session):
    stg_etp_conventionnes = dbt.ref("stg_etp_conventionnes")

    stg_etp_conventionnes["Montant_total_aide"] = (
        stg_etp_conventionnes["effectif_mensuel_conventionné"]
        * stg_etp_conventionnes["af_montant_unitaire_annuel_valeur"]
        / 12
        * stg_etp_conventionnes["duree_annexe"]
    )
    stg_etp_conventionnes["part_conventionnement_cd"] = (
        stg_etp_conventionnes["af_mt_cofinance"] / stg_etp_conventionnes["af_montant_total_annuel"]
    )
    stg_etp_conventionnes["part_conventionnement_cd"] = stg_etp_conventionnes["part_conventionnement_cd"].fillna(0)
    stg_etp_conventionnes["etp_conventionnes_cd"] = (
        stg_etp_conventionnes["effectif_mensuel_conventionné"] * stg_etp_conventionnes["part_conventionnement_cd"]
    ) * (stg_etp_conventionnes["duree_annexe"] / 12)
    # ETP conventionnés Etat
    stg_etp_conventionnes["etp_conventionnes_etat"] = (
        stg_etp_conventionnes["effectif_mensuel_conventionné"]
        * (1 - stg_etp_conventionnes["part_conventionnement_cd"])
    ) * (stg_etp_conventionnes["duree_annexe"] / 12)

    # Nombre de brsa pouvant être subventionnés avec le montant cofinancé (par mois)
    stg_etp_conventionnes["nb_brsa_cible_mensuel"] = (
        stg_etp_conventionnes["af_mt_cofinance"] / stg_etp_conventionnes["duree_annexe"]
    ) / (0.88 * stg_etp_conventionnes["montant_rsa"])
    # idem par an
    stg_etp_conventionnes["nb_brsa_cible_annuel"] = stg_etp_conventionnes["af_mt_cofinance"] / (
        0.88 * stg_etp_conventionnes["montant_rsa"]
    )
    # remplissage des null avec des 0 afin de ne pas casser les scripts
    # dependants de cette table lors du début d'une nouvelle année
    stg_etp_conventionnes["af_mt_cofinance"] = stg_etp_conventionnes["af_mt_cofinance"].fillna(0)
    return stg_etp_conventionnes
