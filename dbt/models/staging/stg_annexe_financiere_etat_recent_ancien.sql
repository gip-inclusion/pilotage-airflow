select
    af_recent.af_id_annexe_financiere,
    af_recent.af_numero_convention,
    af_recent.af_numero_annexe_financiere,
    af_recent.af_etat_annexe_financiere_code,
    af_recent.af_etp_postes_insertion,
    af_recent.af_mesure_dispositif_code,
    af_recent.af_numero_avenant_modification,
    af_recent.af_montant_total_annuel,
    af_recent.af_montant_unitaire_annuel_valeur,
    af_recent.af_mt_cofinance,
    af_recent.type_siae,
    af_recent.denomination_structure,
    af_recent.nom_departement_structure,
    af_recent.nom_region_structure,
    af_recent.most_recent_date as af_date
from {{ ref('stg_annexe_financiere_etat_recent') }} as af_recent

union

select
    af_old.af_id_annexe_financiere,
    af_old.af_numero_convention,
    af_old.af_numero_annexe_financiere,
    af_old.af_etat_annexe_financiere_code,
    af_old.af_etp_postes_insertion,
    af_old.af_mesure_dispositif_code,
    af_old.af_numero_avenant_modification,
    af_old.af_montant_total_annuel,
    af_old.af_montant_unitaire_annuel_valeur,
    af_old.af_mt_cofinance,
    af_old.type_siae,
    af_old.denomination_structure,
    af_old.nom_departement_structure,
    af_old.nom_region_structure,
    af_old.old_date as af_date
from {{ ref('stg_annexe_financiere_etat_ancien') }} as af_old
