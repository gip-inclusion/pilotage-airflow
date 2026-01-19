import pandas

from streamlit_apps.lemarche_mesure_impact_entreprise.load import get_marche_suivi_structure_df


CATEGORY_TYPE_LIST = ["genre_salarie", "contrat_salarie_rqth", "adresse_qpv", "adresse_zrr", "is_senior", "is_junior"]


def verify_uploaded_file(company_data: pandas.DataFrame) -> tuple[bool, list[str]]:
    required_columns = {"SIREN", "Dépense", "Année"}
    missing = required_columns - set(company_data.columns)
    if missing:
        return False, [f"Colonnes manquantes : {', '.join(sorted(missing))}"]
    else:
        return True, []


def filter_structures_data(company_data: pandas.DataFrame, year_to_filter: int) -> pandas.DataFrame:
    siren_list = [str(s) for s in company_data["SIREN"].unique()]
    marche_suivi_structure_df = get_marche_suivi_structure_df(siren_list, year_to_filter)
    marche_suivi_structure_df["siren"] = marche_suivi_structure_df["structure_siret_actualise"].astype(str).str[:9]
    structures_filtered = marche_suivi_structure_df.query("emi_sme_annee == @year_to_filter")

    return structures_filtered


def get_found_sirens(
    company_data: pandas.DataFrame, structures_filtered: pandas.DataFrame, year_to_filter: int
) -> pandas.DataFrame:
    company_data_year = company_data[company_data["Année"] == year_to_filter].copy()

    company_sirens = set(company_data_year["SIREN"].astype(str))

    db_sirens = set(structures_filtered["siren"].astype(str)) if not structures_filtered.empty else set()

    found_sirens = company_sirens & db_sirens

    if found_sirens:
        found_data = company_data_year[company_data_year["SIREN"].astype(str).isin(found_sirens)]
        found_summary = found_data.groupby("SIREN")["Dépense"].sum().reset_index()
        return found_summary
    else:
        return pandas.DataFrame(columns=["SIREN", "Dépense"])


def get_missing_sirens(
    company_data: pandas.DataFrame, structures_filtered: pandas.DataFrame, year_to_filter: int
) -> pandas.DataFrame:
    company_data_year = company_data[company_data["Année"] == year_to_filter].copy()
    company_sirens = set(company_data_year["SIREN"].astype(str))

    db_sirens = set(structures_filtered["siren"].astype(str)) if not structures_filtered.empty else set()

    missing_sirens = company_sirens - db_sirens

    if missing_sirens:
        missing_data = company_data_year[company_data_year["SIREN"].astype(str).isin(missing_sirens)]
        missing_summary = missing_data.groupby("SIREN")["Dépense"].sum().reset_index()
        missing_summary = missing_summary.sort_values("Dépense", ascending=False)
        return missing_summary
    else:
        return pandas.DataFrame(columns=["SIREN", "Dépense"])


def add_cols_to_structures_data(structures_filtered: pandas.DataFrame) -> pandas.DataFrame:
    df = structures_filtered.copy()
    df["is_senior"] = (df["emi_sme_annee"].iloc[0] - df["salarie_annee_naissance"]) >= 55
    df["is_junior"] = (df["emi_sme_annee"].iloc[0] - df["salarie_annee_naissance"]) < 26
    return df


def get_cleaned_structures_data(company_data: pandas.DataFrame, year_to_filter: int) -> pandas.DataFrame:
    cleaned_structure = add_cols_to_structures_data(filter_structures_data(company_data, year_to_filter))
    cleaned_structure[CATEGORY_TYPE_LIST] = cleaned_structure[CATEGORY_TYPE_LIST].fillna("Inconnu").astype(str)
    return cleaned_structure


def filter_company_data_by_structures(
    company_data: pandas.DataFrame, structures_filtered: pandas.DataFrame, year_to_filter: int
) -> pandas.DataFrame:
    filtered_company_data = company_data[
        (company_data["SIREN"].astype(str).isin(structures_filtered["siren"].astype(str)))
        & (company_data["Année"] == year_to_filter)
    ]
    return filtered_company_data


def compute_etp_financed(
    filtered_company_data: pandas.DataFrame, structures_filtered: pandas.DataFrame, count_un_etp: int
) -> tuple[float, float, float, float]:
    etp_financed_by_company = filtered_company_data["Dépense"].sum() / count_un_etp
    montant_financed_by_company = filtered_company_data["Dépense"].sum()
    total_etp_realised = structures_filtered["nombre_etp_consommes_reels_annuels"].sum()
    percentage_etp_financed = (etp_financed_by_company / total_etp_realised) * 100 if total_etp_realised > 0 else 0
    return montant_financed_by_company, etp_financed_by_company, total_etp_realised, percentage_etp_financed


def compute_etp_financed_by_category(
    total_etp_realised: float,
    etp_financed_by_company: float,
    structures_filtered: pandas.DataFrame,
    category_type: str,
) -> pandas.DataFrame:
    category_etp_realised = (
        structures_filtered.groupby(category_type)["nombre_etp_consommes_reels_annuels"]
        .sum()
        .reset_index()
        .rename(columns={category_type: "category"})
    )
    category_etp_realised["category_type"] = category_type
    category_etp_realised["etp_financed_by_company"] = (
        category_etp_realised["nombre_etp_consommes_reels_annuels"] / total_etp_realised
    ) * etp_financed_by_company
    category_etp_realised["perc_etp_financed_by_company_per_category"] = (
        category_etp_realised["etp_financed_by_company"] / etp_financed_by_company
    )

    return category_etp_realised


def compute_beneficiaries_financed_by_category(
    structures_filtered: pandas.DataFrame, category_type: str
) -> pandas.DataFrame:
    beneficiaires_etp = (
        structures_filtered.groupby("hash_nir")["nombre_etp_consommes_reels_annuels"].sum().reset_index()
    )

    beneficiaires_etp = beneficiaires_etp[beneficiaires_etp["nombre_etp_consommes_reels_annuels"] > 0]

    structures_filtered_nonzero_etp = structures_filtered[
        structures_filtered["hash_nir"].isin(beneficiaires_etp["hash_nir"])
    ]

    category_beneficiaries = (
        structures_filtered_nonzero_etp.groupby(category_type)["hash_nir"]
        .nunique()
        .reset_index()
        .rename(columns={category_type: "category", "hash_nir": "nombre_beneficiaires"})
    )
    category_beneficiaries["category_type"] = category_type
    return category_beneficiaries


def merge_and_get_indicators_by_category(
    total_etp_realised: float,
    etp_financed_by_company: float,
    structures_filtered: pandas.DataFrame,
    category_type: str,
) -> pandas.DataFrame:
    category_etp_realised = compute_etp_financed_by_category(
        total_etp_realised, etp_financed_by_company, structures_filtered, category_type
    )
    category_beneficiaries = compute_beneficiaries_financed_by_category(structures_filtered, category_type)
    category_indicators = category_etp_realised.merge(
        category_beneficiaries, how="inner", on=["category_type", "category"]
    )
    category_indicators["beneficiaries_financed_by_company"] = (
        category_indicators["etp_financed_by_company"]
        * category_indicators["nombre_beneficiaires"]
        / category_indicators["nombre_etp_consommes_reels_annuels"]
    )

    return category_indicators


def get_etp_financed_table_by_categories(
    total_etp_realised: float, etp_financed_by_company: float, structures_filtered: pandas.DataFrame
) -> pandas.DataFrame:
    etp_financed_by_categories = pandas.concat(
        [
            merge_and_get_indicators_by_category(
                total_etp_realised, etp_financed_by_company, structures_filtered, category_type
            )
            for category_type in CATEGORY_TYPE_LIST
        ],
        ignore_index=True,
    )
    etp_financed_by_categories = etp_financed_by_categories.rename(
        columns={
            "etp_financed_by_company": "Nombre ETP",
            "perc_etp_financed_by_company_per_category": "Ratio ETP",
            "beneficiaries_financed_by_company": "Nombre beneficiaires",
        }
    )
    return etp_financed_by_categories[["category_type", "category", "Nombre ETP", "Ratio ETP", "Nombre beneficiaires"]]
