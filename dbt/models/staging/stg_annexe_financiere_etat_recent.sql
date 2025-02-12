select
    af_id_annexe_financiere,
    af_numero_convention,
    af_numero_annexe_financiere,
    af_etat_annexe_financiere_code,
    af_etp_postes_insertion,
    af_mesure_dispositif_code,
    af_numero_avenant_modification,
    af_montant_total_annuel,
    af_montant_unitaire_annuel_valeur,
    af_mt_cofinance,
    type_siae,
    denomination_structure,
    nom_departement_structure,
    nom_region_structure,
    case
        when (
            af_etat_annexe_financiere_code in ('VALIDE', 'CLOTURE') and to_date(af_date_etat_valide, 'DD/MM/YYYY')
            <= to_date(af_date_fin_effet, 'DD/MM/YYYY')
        ) then to_date(af_date_etat_valide, 'DD/MM/YYYY')
        when (
            af_etat_annexe_financiere_code in ('VALIDE', 'CLOTURE') and to_date(af_date_etat_valide, 'DD/MM/YYYY')
            > to_date(af_date_fin_effet, 'DD/MM/YYYY')
        ) then to_date(af_date_fin_effet, 'DD/MM/YYYY')
        when af_etat_annexe_financiere_code = 'PROVISOIRE' then to_date(af_date_etat_provisoire, 'DD/MM/YYYY')
    end as most_recent_date
from {{ ref('fluxIAE_AnnexeFinanciere_v2') }}
where af_etat_annexe_financiere_code in ('VALIDE', 'CLOTURE', 'PROVISOIRE')
