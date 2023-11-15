select
    etp.id_annexe_financiere,
    etp.emi_esm_etat_code,
    etp.annee_af,
    etp.type_structure,
    etp.structure_denomination,
    etp.commune_structure,
    etp.code_insee_structure,
    etp.siret_structure,
    etp.code_departement_af,
    etp.nom_departement_af,
    etp.nom_region_af,
    sum(etp.nombre_etp_consommes_reels_mensuels - etp."effectif_mensuel_conventionné")
    as delta_etp_conventionnes_realises,
    sum(etp.nombre_etp_consommes_reels_mensuels)
    as somme_etp_realises
from {{ ref('suivi_realisation_convention_mensuelle') }} as etp
group by
    etp.id_annexe_financiere,
    etp.emi_esm_etat_code,
    etp.annee_af,
    etp.type_structure,
    etp.structure_denomination,
    etp.commune_structure,
    etp.code_insee_structure,
    etp.siret_structure,
    etp.code_departement_af,
    etp.nom_departement_af,
    etp.nom_region_af
