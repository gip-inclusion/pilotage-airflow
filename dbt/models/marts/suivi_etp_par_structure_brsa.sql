select
    etp_r.id_annexe_financiere,
    etp_r.af_numero_annexe_financiere,
    etp_r.emi_sme_mois,
    etp_r.emi_sme_annee,
    etp_r.salarie_brsa,
    etp_r.type_structure,
    etp_r.type_structure_emplois,
    etp_r.structure_denomination,
    etp_r.nom_departement_structure,
    etp_r.nom_region_structure,
    etp_r.code_departement_af,
    etp_r.nom_departement_af,
    etp_r.nom_region_af,
    sum(etp_r.nombre_heures_travaillees)           as nombre_heures_travaillees,
    sum(etp_r.nombre_etp_consommes_reels_annuels)  as etp_consommes_reels_annuels,
    sum(etp_r.nombre_etp_consommes_reels_mensuels) as netp_consommes_reels_mensuels,
    count(etp_r.salarie_brsa)                      as nombre_de_salaries
from
    {{ ref('suivi_etp_realises_v2') }} as etp_r
where nombre_heures_travaillees >= 1
group by
    etp_r.id_annexe_financiere,
    etp_r.af_numero_annexe_financiere,
    etp_r.emi_sme_mois,
    etp_r.emi_sme_annee,
    etp_r.salarie_brsa,
    etp_r.type_structure,
    etp_r.type_structure_emplois,
    etp_r.structure_denomination,
    etp_r.nom_departement_structure,
    etp_r.nom_region_structure,
    etp_r.code_departement_af,
    etp_r.nom_departement_af,
    etp_r.nom_region_af
