--- problème remarqué : une même structure peut avoir plusieurs départements/régions
select
    af_id_structure,
    nom_region_af,
    nom_departement_af,
    af_mesure_dispositif_code
from {{ ref('fluxIAE_AnnexeFinanciere_v2') }}
where af_etat_annexe_financiere_code in ('VALIDE', 'PROVISOIRE', 'CLOTURE')
group by af_id_structure, nom_region_af, nom_departement_af, af_mesure_dispositif_code
