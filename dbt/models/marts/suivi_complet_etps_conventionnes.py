import pandas as pd


def current_year():
    import datetime

    today = datetime.date.today()
    year = today.year
    return year


def reindex_fill_zeros(x, year):
    return x.reindex(
        pd.period_range(str(year) + "-01-01", str(year) + "-12-31", freq="M", name="af_date_fin_effet_v2"),
        fill_value=0,
    )


def exploding_data(df):
    # In order to transform one row per structure into one row per month
    # we explode the df using new colum "dates_annexe"
    df_c = df
    df_c["af_date_debut_effet_v2"] = pd.to_datetime(df_c["af_date_debut_effet_v2"])
    df_c["af_date_fin_effet_v2"] = pd.to_datetime(df_c["af_date_fin_effet_v2"])
    df_c["dates_annexe"] = df_c.apply(
        lambda x: pd.date_range(x["af_date_debut_effet_v2"], x["af_date_fin_effet_v2"], freq="M"), axis=1
    )

    df_c = df_c.explode("dates_annexe")
    df_c.reset_index(inplace=True)
    df_c.drop(columns=["index"], inplace=True)
    df_c["af_date_fin_effet_v2"] = df_c["dates_annexe"]
    df_c = df_c[df.columns[:-1]]
    return df_c


def getting_zero_etp(df):
    year = current_year()
    years = (year - 2, year - 1, year)
    df_y = []

    # Not all structures have a funding for the whole year therefore we create
    # a second table with the non contracted months
    # we set all months = 0 in order to compare with df_c
    # and remove the duplicates where df_c[etp] != 0
    for year in years:
        df_y2 = df[df["annee_af"] == year]
        df_y2["af_date_debut_effet_v2"] = pd.to_datetime(df_y2["af_date_debut_effet_v2"])
        df_y2["af_date_fin_effet_v2"] = pd.to_datetime(df_y2["af_date_fin_effet_v2"])

        for i in df_y2.index:
            if df_y2["effectif_mensuel_conventionné"].loc[i] != 0:
                df_y2 = (
                    df_y2.set_index("af_date_fin_effet_v2")
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
                    .apply(lambda x: reindex_fill_zeros(x, year))
                    .reset_index()
                )

        df_y2["af_date_fin_effet_v2"] = df_y2["af_date_fin_effet_v2"].dt.to_timestamp(how="end").dt.normalize()
        df_y2 = df_y2[df.columns[:-1]]
        df_y.append(df_y2)

    return df_y


def cleaning_data(df):
    df = (df.drop_duplicates(subset=["id_annexe_financiere", "af_date_fin_effet_v2"])).sort_values(
        by=["id_annexe_financiere", "af_date_fin_effet_v2"]
    )
    df["année"] = df["af_date_fin_effet_v2"].dt.year
    df["month"] = df["af_date_fin_effet_v2"].dt.month
    return df


def model(dbt, session):
    df = dbt.ref("suivi_etp_conventionnes_v2")
    df_c = exploding_data(df)
    df_y = getting_zero_etp(df)
    df_y = pd.concat(df_y)
    df = pd.concat([df_c, df_y])
    df = cleaning_data(df)
    return df
