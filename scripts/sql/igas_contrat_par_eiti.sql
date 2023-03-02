drop table if exists igas_contrat_par_eiti;
create table igas_contrat_par_eiti as
select
    contrat_mission.contrat_id_structure as id_struct,
    date_part('year', to_date(contrat_mission.contrat_date_embauche, 'DD/MM/YYYY')) as annee_embauche,
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
    annee_embauche;