from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from calculs import (
    compute_etp_financed,
    filter_company_data_by_structures,
    get_cleaned_structures_data,
    get_etp_financed_table_by_categories,
)


SNAPSHOT_FILE = Path(__file__).resolve().parent / "snapshots" / "snapshot_table_etp_financed.csv"


@patch("streamlit_apps.lemarche_mesure_impact_entreprise.calculs.get_marche_suivi_structure_df")
def test_etp_financed_pipeline(mock_get_structures):
    company_data = pd.DataFrame(
        {"SIREN": ["123456789", "111111111"], "Année": [2023, 2023], "Dépense": [100000, 35000]}
    )

    structures_data = pd.DataFrame(
        {
            "structure_siret_actualise": ["12345678900000", "98765432100000", "12345678900000"],
            "emi_sme_annee": [2023, 2023, 2023],
            "nombre_etp_consommes_reels_annuels": [1.6, 1.4, 1.0],
            "genre_salarie": ["femme", "homme", "homme"],
            "hash_nir": ["id_1", "id_2", "id_2"],
            "salarie_annee_naissance": [1970, 2000, 2000],
            "contrat_salarie_rqth": [True, False, False],
            "adresse_qpv": [True, False, False],
            "adresse_zrr": [False, True, True],
        }
    )

    mock_get_structures.return_value = structures_data

    count_un_etp = 100000

    # pipeline to test
    structures_filtered = get_cleaned_structures_data(company_data, 2023)
    filtered_company_data = filter_company_data_by_structures(company_data, structures_filtered, 2023)
    montant, etp, total_etp, percentage = compute_etp_financed(
        filtered_company_data, structures_filtered, count_un_etp
    )
    result_table = get_etp_financed_table_by_categories(total_etp, etp, structures_filtered)
    result_table = result_table.sort_values(["category_type", "category"]).reset_index(drop=True)

    assert montant == 100000
    assert etp == 1
    assert total_etp == 4
    assert percentage == 25.0

    try:
        expected_snapshot = pd.read_csv(SNAPSHOT_FILE)
        pd.testing.assert_frame_equal(result_table, expected_snapshot, check_dtype=False)
    except FileNotFoundError:
        result_table.to_csv(SNAPSHOT_FILE, index=False)
        pytest.fail(f"Snapshot créé : {SNAPSHOT_FILE}. Vérifie et relance le test.")
