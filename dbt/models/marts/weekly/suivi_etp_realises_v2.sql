select distinct
    emi.emi_pph_id                                                                      as identifiant_salarie,
    emi.emi_afi_id                                                                      as id_annexe_financiere,
    emi.emi_part_etp                                                                    as nombre_etp_consommes_asp,
    emi.emi_nb_heures_travail                                                           as nombre_heures_travaillees,
    emi.emi_afi_id                                                                      as identifiant_annexe_fin,
    emi.emi_ctr_id,
    emi.emi_dsm_id,
    emi.emi_sme_mois,
    emi.emi_sme_annee,
    emi.emi_esm_etat_code,
    emi.emi_date_modification,
    brsa.majoration_brsa,
    brsa.salarie_brsa,
    af.af_numero_annexe_financiere,
    af.af_numero_convention,
    af.af_etat_annexe_financiere_code,
    af.af_montant_unitaire_annuel_valeur,
    af.af_duree_heure_annuel_etp_id,
    firmi.rmi_libelle,
    firmi.rmi_valeur,
    firmi.rmi_id,
    af.af_mesure_dispositif_code,
    ref_asp.type_structure,
    ref_asp.type_structure_emplois,
    structure.structure_id_siae,
    structure.structure_denomination,
    structure.structure_adresse_admin_commune                                           as commune_structure,
    structure.structure_adresse_admin_code_insee                                        as code_insee_structure,
    structure.structure_siret_actualise                                                 as siret_structure,
    structure.nom_epci_structure                                                        as epci_structure,
    structure.zone_emploi_structure,
    structure.nom_departement_structure,
    structure.nom_region_structure,
    af.num_dep_af                                                                       as code_departement_af,
    af.nom_departement_af,
    af.nom_region_af,
    make_date(cast(emi.emi_sme_annee as integer), cast(emi.emi_sme_mois as integer), 1) as date_saisie,
    to_date(emi.emi_date_validation, 'dd/mm/yyyy')                                      as date_validation_declaration,
    /*Nous calculons directement les ETPs réalisés pour éviter des problèmes de filtres/colonnes/etc sur metabase*/
    /* ETPs réalisés = Nbr heures travaillées / montant d'heures necessaires pour avoir 1 ETP */
    -- Ici le calcul nb heures * valeur nous donne de base des ETPs ANNUELS.
    (emi.emi_nb_heures_travail / firmi.rmi_valeur)
        as nombre_etp_consommes_reels_annuels,
    -- multiplication par 12 pour tomber sur le mensuel
    (emi.emi_nb_heures_travail / firmi.rmi_valeur) * 12
        as nombre_etp_consommes_reels_mensuels
from
    {{ ref('eph_dates_etat_mensuel_individualise') }} as constantes
cross join
    {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
left join {{ ref('fluxIAE_AnnexeFinanciere_v2') }} as af
    on
        emi.emi_afi_id = af.af_id_annexe_financiere
        and constantes.annee_en_cours_2 <= emi.emi_sme_annee
left join {{ ref('stg_ref_montant_heures_iae') }} as firmi
    on
    --be careful, there are two columns on annexefinanviere_v2 that match rmi_id.
    -- You need to chose the right column based on the code (detailled on the asp doc)
        af.af_duree_heure_annuel_etp_id = firmi.rmi_id
left join {{ ref('fluxIAE_Structure_v2') }} as structure
    on
        af.af_id_structure = structure.structure_id_siae
left join {{ ref('ref_mesure_dispositif_asp') }} as ref_asp
    on
        af.af_mesure_dispositif_code = ref_asp.af_mesure_dispositif_code
left join {{ ref('stg_etat_mensuel_individuel_avec_brsa') }} as brsa
    on
        emi.emi_afi_id = brsa.emi_afi_id
        and emi.emi_pph_id = brsa.emi_pph_id
        and emi.emi_sme_annee = brsa.emi_sme_annee
        and emi.emi_sme_mois = brsa.emi_sme_mois
        and emi.emi_dsm_id = brsa.emi_dsm_id
where
    emi.emi_sme_annee >= constantes.annee_en_cours_2
    and af.af_etat_annexe_financiere_code in ('VALIDE', 'PROVISOIRE', 'CLOTURE')
    and af.af_mesure_dispositif_code not like '%FDI%'
