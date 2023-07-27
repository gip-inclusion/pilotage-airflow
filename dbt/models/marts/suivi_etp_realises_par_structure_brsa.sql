select
    serv.id_annexe_financiere,
    serv.date_saisie,
    serv.emi_sme_mois,
    serv.emi_sme_annee,
    serv.date_validation_declaration,
    serv.af_numero_annexe_financiere,
    serv.rmi_libelle,
    serv.rmi_valeur,
    serv.salarie_brsa,
    serv.af_mesure_dispositif_code,
    serv.type_structure,
    etp_c.type_structure_emplois,
    serv.structure_denomination,
    serv.commune_structure,
    serv.code_insee_structure,
    serv.siret_structure,
    serv.nom_departement_structure,
    serv.nom_region_structure,
    serv.code_departement_af,
    serv.nom_departement_af,
    serv.nom_region_af,
    etp_c.annee_af,
    m.mois_classe,
    max(etp_c.nb_brsa_cible_mensuel)              as nb_brsa_cible_mensuel,
    sum(serv.nombre_etp_consommes_asp)            as nombre_etp_consommes_asp,
    sum(serv.nombre_heures_travaillees)           as nombre_heures_travaillees,
    sum(serv.nombre_etp_consommes_reels_annuels)  as nombre_etp_consommes_reels_annuels,
    sum(serv.nombre_etp_consommes_reels_mensuels) as nombre_etp_consommes_reels_mensuels,
    count(serv.salarie_brsa)                      as nombre_de_salaries
from
    {{ ref('suivi_etp_realises_v2') }} as serv
left join {{ ref('suivi_etp_conventionnes_v2') }} as etp_c
    on
        serv.id_annexe_financiere = etp_c.id_annexe_financiere
left join {{ ref('months') }} as m
    on
        date_part('month', serv.date_saisie) = m.numero_mois
where nombre_heures_travaillees >= 1
group by
    serv.id_annexe_financiere,
    serv.date_saisie,
    serv.emi_sme_mois,
    serv.emi_sme_annee,
    serv.date_validation_declaration,
    serv.salarie_brsa,
    etp_c.annee_af,
    m.mois_classe,
    serv.af_numero_annexe_financiere,
    serv.rmi_libelle,
    serv.rmi_valeur,
    serv.af_mesure_dispositif_code,
    serv.type_structure,
    etp_c.type_structure_emplois,
    serv.structure_denomination,
    serv.commune_structure,
    serv.code_insee_structure,
    serv.siret_structure,
    serv.nom_departement_structure,
    serv.nom_region_structure,
    serv.code_departement_af,
    serv.nom_departement_af,
    serv.nom_region_af
