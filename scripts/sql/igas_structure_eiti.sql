create table igas_eiti_structure as 
-- etp conventionnes 
with etp_conv_par_struct as (
    select distinct
        structure.structure_id_siae as id_struct,
        structure.structure_denomination as denomination_structure,
        af.af_numero_annexe_financiere as id_annexe_financiere,
        date_part('year',
            af.af_date_debut_effet_v2) as annee_af,
        sum(af.af_etp_postes_insertion) as effectif_mensuel_conv,
        sum((af.af_etp_postes_insertion * ((date_part('year',
                    af_date_fin_effet_v2) - date_part('year',
                    af_date_debut_effet_v2)) * 12 + (date_part('month',
                    af_date_fin_effet_v2) - date_part('month',
                    af_date_debut_effet_v2)) + 1) / 12)) as effectif_annuel_conv
    from
        "fluxIAE_AnnexeFinanciere_v2" as af
    left join "fluxIAE_Structure_v2" as structure on af.af_id_structure = structure.structure_id_siae
    where
        af.af_mesure_dispositif_code = 'EITI_DC'
        --date_part('year', af.af_date_debut_effet_v2) >= annee_en_cours - 2
        and af.af_etat_annexe_financiere_code in('VALIDE',
            'PROVISOIRE',
            'CLOTURE')
        and af_mesure_dispositif_code not like '%FDI%'
    group by
        id_struct,
        structure.structure_denomination,
        af.af_numero_annexe_financiere,
        annee_af
),
-- etp realises
etp_cons_par_struct as (
    select
        structure.structure_id_siae as id_struct,
        date_part('year',
            af.af_date_debut_effet_v2) as annee_af,
        sum(emi.emi_part_etp) as nombre_etp_consommes_asp,
        sum(emi.emi_nb_heures_travail) as nombre_heures_travaillees,
        sum(emi.emi_nb_heures_travail / firmi.rmi_valeur) as nombre_etp_consommes_reels_mensuels,
        sum(emi.emi_nb_heures_travail / firmi.rmi_valeur * 12) as nombre_etp_consommes_reels_annuels
    from
        "fluxIAE_EtatMensuelIndiv" as emi
    left join "fluxIAE_AnnexeFinanciere_v2" as af on emi.emi_afi_id = af.af_id_annexe_financiere
    left join "fluxIAE_RefMontantIae" firmi on af_mesure_dispositif_id = firmi.rme_id
    left join "fluxIAE_Structure_v2" as structure on af.af_id_structure = structure.structure_id_siae
    where
        af.af_mesure_dispositif_code = 'EITI_DC'
        and firmi.rmi_libelle = 'Nombre d''heures annuelles théoriques pour un salarié à taux plein'
        and af.af_etat_annexe_financiere_code in('VALIDE',
            'PROVISOIRE',
            'CLOTURE')
        and af_mesure_dispositif_code not like '%FDI%'
    group by
        id_struct,
        annee_af
),
-- accompagnement spécifique par structure
acc_par_structure as (
select 
    structure.structure_id_siae id_struct,
    sum(acc_dif_nb_sal_conc_illetrisme) nb_salaries_concernes_illetrisme,
    sum(acc_dif_nb_sal_int_ext_illetrisme) nb_salaries_accompagnes_illetrisme,
    sum(acc_dif_nb_sal_conc_sante) nb_salaries_concernes_sante,
    sum(acc_dif_nb_sal_int_ext_sante) nb_salaries_accompagnes_sante,
    sum(acc_dif_nb_sal_conc_heberg) nb_salaries_concernes_hebergement,
    sum(acc_dif_nb_sal_int_ext_heberg) nb_salaries_accompagnes_hebergement,
    sum(acc_dif_nb_sal_conc_demarch) nb_salaries_concernes_demarches_admin,
    sum("acc_dif_nb_sal_int_ext_demarch ") nb_salaries_accompagnes_demarches_admin,
    sum(acc_dif_nb_sal_conc_mobilite) nb_salaries_concernes_mobilite,
    sum(acc_dif_nb_sal_int_ext_mobilite) nb_salaries_accompagnes_mobilite,
    sum(acc_dif_nb_sal_conc_surendet) nb_salaries_concernes_surendetement,
    sum(acc_dif_nb_sal_int_ext_surendet) nb_salaries_accompagnes_surendetement,
    -- bug avec les deux colonnes ci dessous oO
    --sum("acc_dif_nb_sal_conc_justice ") nb_salaries_concernes_justice,
    --sum("acc.acc_dif_nb_sal_int_ext_justice ") nb_salaries_accompagnes_justice,
    sum(acc_dif_nb_sal_conc_manque_dispo) nb_salaries_concernes_manque_dispo,
    sum(acc_dif_nb_sal_int_ext_manque_dispo) nb_salaries_accompagnes_manque_dispo
    from
    "FluxIAE_accompagnement" acc
    left join "fluxIAE_AnnexeFinanciere_v2" as af on af.af_id_annexe_financiere = acc.acc_afi_id
    left join "fluxIAE_Structure_v2" as structure on af.af_id_structure = structure.structure_id_siae
    where af.type_siae = 'EITI'
    group by structure.structure_id_siae
    order by structure.structure_id_siae
),
-- contrats par structure
contrat_par_structure as (
    select
        contrat_mission.contrat_id_structure as id_struct,       
        date_part('year',
            to_date(contrat_mission.contrat_date_embauche,
                'DD/MM/YYYY')) as annee_embauche,
        count(*) as nb_contrats,
        count(formations.*) as nb_formations,
        sum(formations.formation_duree_jours) as nb_jours_formation,
        sum(formations.formation_duree_heures) as nb_heures_formation,
        sum(formations.formation_duree_minutes) as nb_min_formation
    from
        "fluxIAE_ContratMission" contrat_mission
    left join "fluxIAE_Formations" formations on contrat_mission.contrat_id_ctr = formations.formation_id_ctr
    left join "fluxIAE_Structure_v2" structures on contrat_mission.contrat_id_structure = structures.structure_id_siae
where
    contrat_mission.contrat_mesure_disp_code = 'EITI_DC'
group by
    contrat_mission.contrat_id_structure,
    annee_embauche
),
sorties_par_structure as (
    select
        id_structure_asp id_struct,
        annee_sortie,
        -- uniquement les motifs utilisés en eiti
        count(motif_sortie) filter (where motif_sortie = 'Sortie automatique') as sortie_automatique,
        count(motif_sortie) filter (where motif_sortie = 'Création ou reprise d''entreprise à son compte (hors EITI)') as sortie_creation_entreprise_hors_eiti,
        count(motif_sortie) filter (where motif_sortie = 'Sans nouvelle') as sortie_sans_nouvelle,
        count(motif_sortie) filter (where motif_sortie = 'Inactif') as sortie_inactif,
        count(motif_sortie) filter (where motif_sortie = 'Autre sortie reconnue comme positive') as sortie_positive,
        count(motif_sortie) filter (where motif_sortie = 'Poursuite au sein de l''EITI sans aide au poste avec un CA supérieur à 70% du salaire médian mensuel') as sortie_poursuite_eiti_median,
        count(motif_sortie) filter (where motif_sortie = 'Embauche en CDI non aidé') as sortie_cdi_non_aidé,
        count(motif_sortie) filter (where motif_sortie = 'Transfert d''employeur') as sortie_transfert_employeur,
        count(motif_sortie) filter (where motif_sortie = 'Entrée en formation qualifiante ou poursuite de formation qualifiante') as sortie_formation_qualifiante,
        count(motif_sortie) filter (where motif_sortie = 'Embauche en CDD (sans aide publique à l''emploi) de moins de 6 mois (hors IAE)') as sortie_cdd_moins_6mois_hors_iae,
        count(motif_sortie) filter (where motif_sortie = 'Embauche en CDD (sans aide publique à l''emploi) d''une durée de 6 mois et plus') as sortie_cdd_plus_6mois,
        count(motif_sortie) filter (where motif_sortie = 'Au chômage') as sortie_chomage,
        count(motif_sortie) filter (where motif_sortie = 'Poursuite au sein de l''EITI sans aide au poste avec un CA supérieur au seuil de pauvreté') as sortie_poursuite_eiti_minimum,
        count(motif_sortie) filter (where motif_sortie = 'Prise des droits à la retraite') as sortie_retraite,
        count(motif_sortie) filter (where motif_sortie = 'Embauche pour une durée déterminée dans une autre structure IAE') as sortie_cdd_iae,
        count(motif_sortie) filter (where motif_sortie = 'Rupture employeur pour faute grave du salarié') as sortie_faute_grave,
        count(motif_sortie) filter (where motif_sortie = 'Embauche en CDI aidé') as sortie_cdi_aide,
        count(motif_sortie) filter (where motif_sortie = 'Décision administrative / Décision de justice') as sortie_decision_admin
    from
        sorties
    where
        sorties.type_siae = 'EITI'
    group by
        id_structure_asp,
        annee_sortie
)
select
    cps.id_struct,
    conv.denomination_structure,
    conv.id_annexe_financiere,
    -- contrats
    cps.nb_contrats,
    cps.nb_formations,
    cps.nb_jours_formation,
    cps.nb_heures_formation,
    cps.nb_min_formation,
    -- conventionnements
    conv.effectif_annuel_conv,
    cons.nombre_etp_consommes_reels_annuels,
    cons.nombre_etp_consommes_asp,
    conv.effectif_mensuel_conv,
    cons.nombre_etp_consommes_reels_mensuels,
    -- accompagnement par structure
    aps.nb_salaries_concernes_illetrisme,
    aps.nb_salaries_accompagnes_illetrisme,
    aps.nb_salaries_concernes_sante,
    aps.nb_salaries_accompagnes_sante,
    aps.nb_salaries_concernes_hebergement,
    aps.nb_salaries_accompagnes_hebergement,
    aps.nb_salaries_concernes_demarches_admin,
    aps.nb_salaries_accompagnes_demarches_admin,
    aps.nb_salaries_concernes_mobilite,
    aps.nb_salaries_accompagnes_mobilite,
    aps.nb_salaries_concernes_surendetement,
    aps.nb_salaries_accompagnes_surendetement,

    aps.nb_salaries_concernes_manque_dispo,
    aps.nb_salaries_accompagnes_manque_dispo,
    -- sorties par structure
    sps.sortie_automatique,
    sps.sortie_creation_entreprise_hors_eiti,
    sps.sortie_sans_nouvelle,
    sps.sortie_inactif,
    sps.sortie_positive,
    sps.sortie_cdi_non_aidé,
    sps.sortie_transfert_employeur,
    sps.sortie_formation_qualifiante,
    sps.sortie_cdd_moins_6mois_hors_iae,
    sps.sortie_cdd_plus_6mois,
    sps.sortie_chomage,
    sps.sortie_poursuite_eiti_median,
    sps.sortie_poursuite_eiti_minimum,
    sps.sortie_retraite,
    sps.sortie_cdd_iae,
    sps.sortie_faute_grave,
    sps.sortie_cdi_aide,
    sps.sortie_decision_admin
from
    contrat_par_structure cps
    left join sorties_par_structure sps on cps.id_struct = sps.id_struct and sps.annee_sortie = cps.annee_embauche
    left join acc_par_structure aps on cps.id_struct = aps.id_struct 
    left join etp_cons_par_struct cons on cps.id_struct = cons.id_struct and cps.annee_embauche = cons.annee_af
    left join etp_conv_par_struct conv on cons.id_struct = conv.id_struct
        and cons.annee_af = conv.annee_af;