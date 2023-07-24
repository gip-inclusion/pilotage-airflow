import datetime

import pandas as pd


def replicate_rows(df):
    # In order to transform one row per structure into one row per month
    # we explode the df using new colum "dates_annexe"
    df_replicate = df
    df_replicate["af_date_debut_effet_v2"] = pd.to_datetime(df_replicate["af_date_debut_effet_v2"])
    df_replicate["af_date_fin_effet_v2"] = pd.to_datetime(df_replicate["af_date_fin_effet_v2"])
    df_replicate["dates_annexe"] = df_replicate.apply(
        lambda x: pd.date_range(x["af_date_debut_effet_v2"], x["af_date_fin_effet_v2"], freq="M"), axis=1
    )

    df_replicate = df_replicate.explode("dates_annexe")
    df_replicate.reset_index(inplace=True)
    df_replicate.drop(columns=["index"], inplace=True)
    df_replicate["af_date_fin_effet_v2"] = df_replicate["dates_annexe"]
    df_replicate = df_replicate[df.columns[:-1]]
    return df_replicate


def get_zero_etp(df):
    def fn_inline(x, year):
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
                            "structure_denomination",
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
                    .apply(lambda x: fn_inline(x, year))
                    .reset_index()
                )

        df_temp["af_date_fin_effet_v2"] = df_temp["af_date_fin_effet_v2"].dt.to_timestamp(how="end").dt.normalize()
        df_temp = df_temp[df.columns[:-1]]
        df_etp_null.append(df_temp)

    return df_etp_null


def join_data(df_replicate, df_etp_null):
    df = pd.concat([df_replicate, df_etp_null])
    df = (df.drop_duplicates(subset=["id_annexe_financiere", "af_date_fin_effet_v2"])).sort_values(
        by=["id_annexe_financiere", "af_date_fin_effet_v2"]
    )
    df["année"] = df["af_date_fin_effet_v2"].dt.year
    df["month"] = df["af_date_fin_effet_v2"].dt.month
    return df


def model(dbt, session):
    df = dbt.ref("suivi_etp_conventionnes_v2")
    df_replicate = replicate_rows(df)
    df_etp_null = get_zero_etp(df)
    df_etp_null = pd.concat(df_etp_null)
    df = join_data(df_replicate, df_etp_null)
    return df
