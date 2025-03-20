select
    af_mesure_dispositif_code,
    af_id_annexe_financiere,
    af_numero_annexe_financiere,
    af_id_structure,
    af_date_debut_effet_v2,
    af_date_fin_effet_v2
from {{ ref('fluxIAE_AnnexeFinanciere_v2') }}
where af_etat_annexe_financiere_code in ('VALIDE', 'PROVISOIRE', 'CLOTURE') and af_mesure_dispositif_code != 'FDI_DC'
group by af_mesure_dispositif_code, af_id_annexe_financiere, af_numero_annexe_financiere, af_id_structure, af_date_debut_effet_v2, af_date_fin_effet_v2
