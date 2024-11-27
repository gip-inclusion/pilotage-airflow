import datetime

import pandas as pd


def explode_by_month(df):
    # In order to transform one row per structure into one row per month
    # we explode the df using new colum "dates_annexe"
    # before the explosion we had one row = one structure
    # with all the informations regarding this structure
    # After the explosion we obtain one row per month where the af is valid
    # each row contain the information linked to the associated month

    df["dates_annexe"] = df.apply(
        lambda x: pd.date_range(x["af_date_debut_effet_v2"], x["af_date_fin_effet_v2"], freq="M"), axis=1
    )

    df = df.explode("dates_annexe")
    df.reset_index(inplace=True)
    df.drop(columns=["index"], inplace=True)
    df["af_date_fin_effet_v2"] = df["dates_annexe"]
    df = df.drop(columns="dates_annexe")
    return df


def get_zero_etp(df):
    def fill_with_zeroes(x, year):
        return x.reindex(
            pd.period_range(str(year) + "-01-01", str(year) + "-12-31", freq="M", name="af_date_fin_effet_v2"),
            fill_value=0,
        )

    current_year = datetime.date.today().year
    three_last_years = [current_year - 2, current_year - 1, current_year]
    df_etp_null = []

    # Not all structures have a funding for the whole year therefore we create
    # a second table with the non contracted months
    # we set all months = 0 in order to compare with df_replicate
    # and remove the duplicates where df_replicate[etp] != 0
    for year in three_last_years:
        df_temp = df[df["annee_af"] == year]
        if df_temp.empty:
            continue
        df_temp["af_date_debut_effet_v2"] = pd.to_datetime(df_temp["af_date_debut_effet_v2"])
        df_temp["af_date_fin_effet_v2"] = pd.to_datetime(df_temp["af_date_fin_effet_v2"])

        for i in df_temp.index:
            if df_temp["effectif_mensuel_conventionné"].loc[i] != 0:
                df_temp = (
                    df_temp.set_index("af_date_fin_effet_v2")
                    .groupby(
                        [
                            "af_date_debut_effet_v2",
                            "af_etat_annexe_financiere_code",
                            "af_mesure_dispositif_code",
                            "af_mesure_dispositif_id",
                            "af_numero_annexe_financiere",
                            "af_numero_avenant_modification",
                            "af_numero_convention",
                            "annee_af",
                            "code_departement_af",
                            "code_insee_structure",
                            "commune_structure",
                            "duree_annexe",
                            "id_annexe_financiere",
                            "nom_departement_af",
                            "nom_departement_structure",
                            "nom_region_af",
                            "nom_region_structure",
                            "siret_structure",
                            "type_structure_emplois",
                            "structure_id_siae",
                            "structure_denomination",
                            "structure_denomination_unique",
                            "type_structure",
                            "year_diff",
                            "af_montant_total_annuel",
                            "af_montant_unitaire_annuel_valeur",
                            "af_mt_cofinance",
                            "montant_rsa",
                            "Montant_total_aide",
                            "part_conventionnement_cd",
                            "etp_conventionnes_cd",
                            "etp_conventionnes_etat",
                        ]
                    )[
                        [
                            "effectif_mensuel_conventionné",
                            "effectif_annuel_conventionné",
                            "nb_brsa_cible_mensuel",
                            "nb_brsa_cible_annuel",
                        ]
                    ]
                    .apply(lambda x: fill_with_zeroes(x, year))
                    .reset_index()
                )

        df_temp["af_date_fin_effet_v2"] = df_temp["af_date_fin_effet_v2"].dt.to_timestamp(how="end").dt.normalize()
        df_temp = df_temp[df.columns[:-1]]
        df_etp_null.append(df_temp)

    return df_etp_null


def join_etp_null_and_realized(df_replicate, df_etp_null):
    df = pd.concat([df_replicate, df_etp_null])
    df = (df.drop_duplicates(subset=["id_annexe_financiere", "af_date_fin_effet_v2"])).sort_values(
        by=["id_annexe_financiere", "af_date_fin_effet_v2"]
    )
    df["année"] = df["af_date_fin_effet_v2"].dt.year
    df["month"] = df["af_date_fin_effet_v2"].dt.month
    return df


def model(dbt, session):
    df = dbt.ref("suivi_etp_conventionnes_v2")
    # We don't need these columns
    df.drop(
        columns=[
            "mpu_sct_mt_recet_nettoyage",
            "mpu_sct_mt_recet_serv_pers",
            "mpu_sct_mt_recet_btp",
            "mpu_sct_mt_recet_agri",
            "mpu_sct_mt_recet_recycl",
            "mpu_sct_mt_recet_transp",
            "mpu_sct_mt_recet_autres",
        ],
        inplace=True,
    )
    df["af_date_debut_effet_v2"] = pd.to_datetime(df["af_date_debut_effet_v2"])
    df["af_date_fin_effet_v2"] = pd.to_datetime(df["af_date_fin_effet_v2"])
    df_replicate = explode_by_month(df)
    df_etp_null = get_zero_etp(df)
    df_etp_null = pd.concat(df_etp_null)
    df = join_etp_null_and_realized(df_replicate, df_etp_null)
    return df
