--drop table if exists igas_eiti_structure;
create table igas_eiti_structure as
-- etp conventionnes
with etp_conv_par_struct as (
    select distinct
        structure.structure_id_siae as id_struct,
        structure.structure_siret_actualise as siret,
        structure.structure_denomination as denomination_structure,
        structure.nom_region_structure as region_structure,
        structure.nom_departement_structure as departement_structure,
        date_part('year',
            af.af_date_debut_effet_v2) as annee_af,
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
    and af.af_etat_annexe_financiere_code in('VALIDE',
        'PROVISOIRE',
        'CLOTURE')
    and af_mesure_dispositif_code not like '%FDI%'
group by
    id_struct,
    structure.structure_siret_actualise,
    structure.structure_denomination,
    annee_af,
    structure.nom_region_structure,
    structure.nom_departement_structure
),
-- etp realises
etp_cons_par_struct as (
    select
        structure.structure_id_siae as id_struct,
        date_part('year',
            af.af_date_debut_effet_v2) as annee_af,
        sum(emi.emi_nb_heures_travail) as nombre_heures_travaillees,
        sum(emi.emi_nb_heures_travail / firmi.rmi_valeur) as nombre_etp_consommes_reels_annuels
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
        date_part('year',
            af.af_date_debut_effet_v2) as annee_af,
        sum(acc_dif_nb_sal_conc_illetrisme) nb_salaries_concernes_illetrisme,
        sum(acc_dif_nb_sal_int_illetrisme) nb_salaries_accompagnes_illetrisme_int,
        sum(acc_dif_nb_sal_ext_illetrisme) nb_salaries_accompagnes_illetrisme_ext,
        sum(acc_dif_nb_sal_conc_sante) nb_salaries_concernes_sante,
        sum(acc_dif_nb_sal_int_sante) nb_salaries_accompagnes_sante_int,
        sum(acc_dif_nb_sal_ext_sante) nb_salaries_accompagnes_sante_ext,
        sum(acc_dif_nb_sal_conc_heberg) nb_salaries_concernes_hebergement,
        sum(acc_dif_nb_sal_int_heberg) nb_salaries_accompagnes_hebergement_int,
        sum(acc_dif_nb_sal_ext_heberg) nb_salaries_accompagnes_hebergement_ext,
        sum(acc_dif_nb_sal_conc_demarch) nb_salaries_concernes_demarches_admin,
        sum(acc_dif_nb_sal_int_demarch) nb_salaries_accompagnes_demarches_admin_int,
        sum(acc_dif_nb_sal_ext_demarch) nb_salaries_accompagnes_demarches_admin_ext,
        sum(acc_dif_nb_sal_conc_mobilite) nb_salaries_concernes_mobilite,
        sum(acc_dif_nb_sal_int_mobilite) nb_salaries_accompagnes_mobilite_int,
        sum(acc_dif_nb_sal_ext_mobilite) nb_salaries_accompagnes_mobilite_ext,
        sum(acc_dif_nb_sal_conc_surendet) nb_salaries_concernes_surendetement,
        sum(acc_dif_nb_sal_int_surendet) nb_salaries_accompagnes_surendetement_int,
        sum(acc_dif_nb_sal_ext_surendet) nb_salaries_accompagnes_surendetement_ext,
        sum(acc_dif_nb_sal_ext_justice) nb_salaries_concernes_justice_ext,
        sum(acc_dif_nb_sal_int_justice) nb_salaries_concernes_justice_int,
        sum(acc_dif_nb_sal_conc_manque_dispo) nb_salaries_concernes_manque_dispo,
        sum(acc_dif_nb_sal_int_manque_dispo) nb_salaries_accompagnes_manque_dispo_int,
        sum(acc_dif_nb_sal_ext_manque_dispo) nb_salaries_accompagnes_manque_dispo_ext
    from
        "FluxIAE_accompagnement" acc
    left join "fluxIAE_AnnexeFinanciere_v2" as af on af.af_id_annexe_financiere = acc.acc_afi_id
    left join "fluxIAE_Structure_v2" as structure on af.af_id_structure = structure.structure_id_siae
where
    af.type_siae = 'EITI'
group by
    structure.structure_id_siae,
    annee_af
),
-- contrats par structure
contrat_par_structure as (
    select
        contrat_mission.contrat_id_structure as id_struct,
        date_part('year',
            to_date(contrat_mission.contrat_date_embauche,
                'DD/MM/YYYY')) as annee_embauche,
        count(*) as nb_contrats,
        count(distinct contrat_mission.contrat_id_pph) as nb_salaries,
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
        --categorie sortie
        count(categorie_sortie) filter (where categorie_sortie = 'Emploi durable') as emploi_durable,
        count(categorie_sortie) filter (where categorie_sortie = 'Emploi de transition') as emploi_de_transition,
        count(categorie_sortie) filter (where categorie_sortie = 'Sorties positives') as sorties_positives,
        count(categorie_sortie) filter (where categorie_sortie = 'Autres sorties') as autres_sorties,
        count(categorie_sortie) filter (where categorie_sortie = 'Retrait des sorties constatées') as "retrait_des_sorties_constatées" from sorties where sorties.type_siae = 'EITI'
    group by
        id_structure_asp,
        annee_sortie
)
select
    conv.id_struct,
    cons.annee_af,
    conv.denomination_structure,
    conv.region_structure,
    conv.departement_structure,
    conv.siret,
    -- contrats
    cps.nb_contrats,
    cps.nb_salaries,
    cps.nb_formations,
    cps.nb_jours_formation,
    cps.nb_heures_formation,
    cps.nb_min_formation,
    -- conventionnements
    conv.effectif_annuel_conv,
    cons.nombre_etp_consommes_reels_annuels,
    -- accompagnement par structure
    aps.nb_salaries_concernes_illetrisme,
    aps.nb_salaries_accompagnes_illetrisme_int,
    aps.nb_salaries_accompagnes_illetrisme_ext,
    aps.nb_salaries_concernes_sante,
    aps.nb_salaries_accompagnes_sante_int,
    aps.nb_salaries_accompagnes_sante_ext,
    aps.nb_salaries_concernes_hebergement,
    aps.nb_salaries_accompagnes_hebergement_int,
    aps.nb_salaries_accompagnes_hebergement_ext,
    aps.nb_salaries_concernes_demarches_admin,
    aps.nb_salaries_accompagnes_demarches_admin_int,
    aps.nb_salaries_accompagnes_demarches_admin_ext,
    aps.nb_salaries_concernes_mobilite,
    aps.nb_salaries_accompagnes_mobilite_int,
    aps.nb_salaries_accompagnes_mobilite_ext,
    aps.nb_salaries_concernes_surendetement,
    aps.nb_salaries_accompagnes_surendetement_int,
    aps.nb_salaries_accompagnes_surendetement_ext,
    aps.nb_salaries_concernes_justice_ext,
    aps.nb_salaries_concernes_justice_int,
    aps.nb_salaries_concernes_manque_dispo,
    aps.nb_salaries_accompagnes_manque_dispo_int,
    aps.nb_salaries_accompagnes_manque_dispo_ext,
    -- sorties par structure
    sps.emploi_durable,
    sps.emploi_de_transition,
    sps.sorties_positives,
    sps.autres_sorties,
    sps."retrait_des_sorties_constatées" from etp_conv_par_struct conv
    left join etp_cons_par_struct cons on conv.id_struct = cons.id_struct
        and conv.annee_af = cons.annee_af
    left join acc_par_structure aps on conv.id_struct = aps.id_struct
        and conv.annee_af = aps.annee_af
    left join contrat_par_structure cps on conv.id_struct = cps.id_struct
        and conv.annee_af = cps.annee_embauche
    left join sorties_par_structure sps on conv.id_struct = sps.id_struct
        and conv.annee_af = sps.annee_sortie