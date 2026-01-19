from unittest.mock import patch

import pandas as pd
import pytest

from streamlit_apps.lemarche_mesure_impact_entreprise.calculs import (
    add_cols_to_structures_data,
    compute_beneficiaries_financed_by_category,
    compute_etp_financed,
    compute_etp_financed_by_category,
    filter_company_data_by_structures,
    filter_structures_data,
    get_found_sirens,
    get_missing_sirens,
    verify_uploaded_file,
)


COMPANY_BASE = pd.DataFrame({"SIREN": ["123456789", "111111111"], "Année": [2023, 2023], "Dépense": [100000, 200000]})

MOCK_STRUCTURES = pd.DataFrame(
    {
        "structure_siret_actualise": ["12345678900000", "98765432100000"],
        "emi_sme_annee": [2023, 2023],
        "nombre_etp_consommes_reels_annuels": [1.6, 2.4],
        "hash_nir": ["identifiant_1", "identifiant2"],
        "genre_salarie": ["femme", "homme"],
    }
)

FILTERED_STRUCTURES = pd.DataFrame(
    {
        "structure_siret_actualise": ["12345678900000", "98765432100000"],
        "emi_sme_annee": [2023, 2023],
        "nombre_etp_consommes_reels_annuels": [1.6, 2.4],
        "hash_nir": ["identifiant_1", "identifiant2"],
        "genre_salarie": ["femme", "homme"],
        "siren": ["123456789", "987654321"],
    }
)

COUNT_UN_ETP = 100000
EXPECTED_ETP_FINANCED_BY_COMPANY = 3
EXPECTED_TOTAL_ETP = 4


# -----------------------------------
# Test verify_uploaded_file fonction
# -----------------------------------
@pytest.mark.parametrize(
    "df,expected_valid,expected_errors",
    [
        (pd.DataFrame({"SIREN": ["123"], "Dépense": [100], "Année": [2023]}), True, []),
        (pd.DataFrame({"SIREN": ["123"], "Dépense": [100], "An": [2023]}), False, ["Colonnes manquantes : Année"]),
        (pd.DataFrame({"Dépense": [100], "Année": [2023]}), False, ["Colonnes manquantes : SIREN"]),
        (pd.DataFrame({}), False, ["Colonnes manquantes : Année, Dépense, SIREN"]),
    ],
)
def test_verify_uploaded(df, expected_valid, expected_errors):
    is_valid, errors = verify_uploaded_file(df)
    assert is_valid == expected_valid
    assert errors == expected_errors


# -------------------------------------
# Test filter_structures_data function
# -------------------------------------


@pytest.mark.parametrize(
    "company_data,year_to_filter,mock_df,expected_df",
    [
        (COMPANY_BASE, 2023, MOCK_STRUCTURES, FILTERED_STRUCTURES),
        (COMPANY_BASE, 2022, MOCK_STRUCTURES, FILTERED_STRUCTURES.iloc[0:0]),
    ],
)
@patch("streamlit_apps.lemarche_mesure_impact_entreprise.calculs.get_marche_suivi_structure_df")
def test_filter_structures_data(mock_get_structures, company_data, year_to_filter, mock_df, expected_df):
    mock_get_structures.return_value = mock_df

    result = filter_structures_data(company_data, year_to_filter)

    result_sorted = result.sort_values("structure_siret_actualise").reset_index(drop=True)
    expected_sorted = expected_df.sort_values("structure_siret_actualise").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_sorted, expected_sorted)


# ---------------------------------
# test add_cols_to_structures_data
# ---------------------------------


@pytest.mark.parametrize(
    "input_df, expected_senior, expected_junior",
    [
        (pd.DataFrame({"emi_sme_annee": [2023], "salarie_annee_naissance": [1960]}), [True], [False]),
        (pd.DataFrame({"emi_sme_annee": [2023], "salarie_annee_naissance": [2000]}), [False], [True]),
        (pd.DataFrame({"emi_sme_annee": [2023], "salarie_annee_naissance": [1985]}), [False], [False]),
    ],
)
def test_add_cols_to_structures_data(input_df, expected_senior, expected_junior):
    result = add_cols_to_structures_data(input_df)

    assert result["is_senior"].tolist() == expected_senior
    assert result["is_junior"].tolist() == expected_junior


# #---------------------------------------
# # test filter_company_data_by_structures
# #---------------------------------------


@pytest.mark.parametrize(
    "company_data, structures_filtered, year_to_filter, expected_siren",
    [
        (COMPANY_BASE, pd.DataFrame({"siren": ["123456789"]}), 2023, ["123456789"]),
        (COMPANY_BASE, pd.DataFrame({"siren": ["123456789"]}), 2022, []),
    ],
)
def test_filter_company_data_by_structures(company_data, structures_filtered, year_to_filter, expected_siren):
    result = filter_company_data_by_structures(company_data, structures_filtered, year_to_filter)
    assert result["SIREN"].tolist() == expected_siren


# #----------------------------------
# # Test compute_etp_financed
# #---------------------------


@pytest.mark.parametrize(
    "company_data, structures_data, expected_montant, expected_etp, expected_total_etp, expected_percentage",
    [
        (COMPANY_BASE, FILTERED_STRUCTURES, 300000, EXPECTED_ETP_FINANCED_BY_COMPANY, EXPECTED_TOTAL_ETP, 75.0),
        (
            COMPANY_BASE,
            pd.DataFrame({"nombre_etp_consommes_reels_annuels": [0]}),
            300000,
            EXPECTED_ETP_FINANCED_BY_COMPANY,
            0,
            0.0,
        ),
    ],
)
def test_compute_etp_financed(
    company_data, structures_data, expected_montant, expected_etp, expected_total_etp, expected_percentage
):
    montant, etp, total_etp, percentage = compute_etp_financed(company_data, structures_data, COUNT_UN_ETP)
    assert pytest.approx(montant) == expected_montant
    assert pytest.approx(etp) == expected_etp
    assert pytest.approx(total_etp) == expected_total_etp
    assert pytest.approx(percentage) == expected_percentage


# #---------------------------------------
# # Test compute etp financed by category
# # --------------------------------------


def test_compute_etp_financed_by_category():
    category_type = "genre_salarie"

    expected = pd.DataFrame(
        {
            "category": ["femme", "homme"],
            "nombre_etp_consommes_reels_annuels": [1.6, 2.4],
            "category_type": ["genre_salarie", "genre_salarie"],
            "etp_financed_by_company": [1.2, 1.8],
            "perc_etp_financed_by_company_per_category": [0.4, 0.6],
        }
    )

    result = compute_etp_financed_by_category(
        EXPECTED_TOTAL_ETP, EXPECTED_ETP_FINANCED_BY_COMPANY, FILTERED_STRUCTURES, category_type
    )

    result = result.sort_values("category").reset_index(drop=True)
    expected = expected.sort_values("category").reset_index(drop=True)

    pd.testing.assert_frame_equal(result, expected)

    assert pytest.approx(result["perc_etp_financed_by_company_per_category"].sum()) == 1.0


# #-------------------------------------------------
# # Test compute beneficiaries_financed_by_category
# # ------------------------------------------------


def test_compute_beneficiaries_financed_by_category():
    df = pd.DataFrame(
        {
            "adresse_qpv": ["False", "True", "False", "False", "False", "True"],
            "hash_nir": ["id_0", "id_1", "id_2", "id_2", "id_3", "id_1"],
            "nombre_etp_consommes_reels_annuels": [0, 0.3, 1, 0.3, 0.25, 0.50],
        }
    )

    result = (
        compute_beneficiaries_financed_by_category(structures_filtered=df, category_type="adresse_qpv")
        .sort_values("category")
        .reset_index(drop=True)
    )

    expected = pd.DataFrame(
        {
            "category": ["False", "True"],
            "nombre_beneficiaires": [2, 1],
            "category_type": ["adresse_qpv", "adresse_qpv"],
        }
    )

    pd.testing.assert_frame_equal(result, expected)


# ========================================
# Tests pour get_missing_sirens
# ========================================


def test_get_missing_sirens_with_missing_data():
    """Test avec des SIRENs manquants dans la base de données"""
    company_data = pd.DataFrame(
        {
            "SIREN": ["123456789", "987654321", "111111111"],
            "Année": [2023, 2023, 2023],
            "Dépense": [100000, 50000, 75000],
        }
    )

    structures_filtered = pd.DataFrame(
        {
            "siren": ["123456789"],  # Seulement un SIREN présent
            "structure_siret_actualise": ["12345678900000"],
            "emi_sme_annee": [2023],
        }
    )

    result = get_missing_sirens(company_data, structures_filtered, 2023)

    # Vérifier que deux SIRENs sont manquants
    assert len(result) == 2
    assert set(result["SIREN"].astype(str)) == {"987654321", "111111111"}

    # Vérifier les montants
    assert result[result["SIREN"] == "987654321"]["Dépense"].to_numpy()[0] == 50000
    assert result[result["SIREN"] == "111111111"]["Dépense"].to_numpy()[0] == 75000

    # Vérifier le tri par montant décroissant
    assert result["Dépense"].tolist() == [75000, 50000]


# ========================================
# Tests pour get_found_sirens
# ========================================


def test_get_found_sirens_with_found_data():
    """Test avec des SIRENs trouvés dans la base de données"""
    company_data = pd.DataFrame(
        {
            "SIREN": ["123456789", "987654321", "111111111"],
            "Année": [2023, 2023, 2023],
            "Dépense": [100000, 50000, 75000],
        }
    )

    structures_filtered = pd.DataFrame(
        {
            "siren": ["123456789", "987654321"],  # Deux SIRENs présents
            "structure_siret_actualise": ["12345678900000", "98765432100000"],
            "emi_sme_annee": [2023, 2023],
        }
    )

    result = get_found_sirens(company_data, structures_filtered, 2023)

    # Vérifier que deux SIRENs sont trouvés
    assert len(result) == 2
    assert set(result["SIREN"].astype(str)) == {"123456789", "987654321"}

    # Vérifier les montants
    assert result[result["SIREN"] == "123456789"]["Dépense"].to_numpy()[0] == 100000
    assert result[result["SIREN"] == "987654321"]["Dépense"].to_numpy()[0] == 50000
