with etp_par_structure as (
    select
        af.af_id_structure as id_struct,
        date_part('year', etp_r.date_validation_declaration) as annee_declaration,        
        sum(etp_r.nombre_etp_consommes_asp) as etp_consommes_asp,
        sum(etp_r.nombre_etp_consommes_reels_annuels) as nombre_etp_consommes_reels_annuels,
        sum(etp_r.nombre_etp_consommes_reels_mensuels) as nombre_etp_consommes_reels_mensuels
    from
        "suivi_etp_realises_v2" etp_r
    left join "fluxIAE_AnnexeFinanciere_v2" af on af.af_id_annexe_financiere = etp_r.id_annexe_financiere
where
    type_structure = 'EITI DC'
group by
    af.af_id_structure,
    annee_declaration
),
contrat_par_structure as (
    select
        contrat_mission.contrat_id_structure as id_struct,
        date_part('year', to_date(contrat_mission.contrat_date_embauche, 'DD/MM/YYYY')) as annee_embauche,
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
    *
from
    etp_par_structure etp_s
    left join contrat_par_structure contrat_s on etp_s.id_struct = contrat_s.id_struct and etp_s.annee_declaration = contrat_s.annee_embauche;