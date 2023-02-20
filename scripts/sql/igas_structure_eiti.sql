-- etp conventionnes 
with etp_conv_par_struct as (
    select distinct
        structure.structure_id_siae as id_struct,
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
)
select
    cps.*,
    conv.effectif_annuel_conv,
    cons.nombre_etp_consommes_reels_annuels,
    cons.nombre_etp_consommes_asp,
    conv.effectif_mensuel_conv,
    cons.nombre_etp_consommes_reels_mensuels,
    aps.* -- accompagnement par structure
from
    contrat_par_structure cps
    left join acc_par_structure aps on cps.id_struct = aps.id_struct
    left join etp_cons_par_struct cons on cps.id_struct = cons.id_struct and cps.annee_embauche = cons.annee_af
    left join etp_conv_par_struct conv on cons.id_struct = conv.id_struct
        and cons.annee_af = conv.annee_af;