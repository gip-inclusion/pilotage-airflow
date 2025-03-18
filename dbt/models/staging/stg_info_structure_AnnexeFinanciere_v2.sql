select
    af_id_structure,
    af_mesure_dispositif_code,
    array_agg(distinct nom_region_af)      as noms_regions_af,
    array_agg(distinct nom_departement_af) as noms_departements_af,
    count(distinct nom_region_af)          as nombre_regions_af,
    count(distinct nom_departement_af)     as nombre_departements_af
from {{ ref('fluxIAE_AnnexeFinanciere_v2') }}
where af_etat_annexe_financiere_code in ('VALIDE', 'PROVISOIRE', 'CLOTURE') and af_mesure_dispositif_code != 'FDI_DC'
group by af_id_structure, af_mesure_dispositif_code
