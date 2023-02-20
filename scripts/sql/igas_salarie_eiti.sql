with formations_par_contrat as (
    select
        formations.formation_id_ctr,
        count(formations.*) as nb_formations,
        sum(formations.formation_duree_jours) as nb_jours_formation,
        sum(formations.formation_duree_heures) as nb_heures_formation,
        sum(formations.formation_duree_minutes) as nb_min_formation
    from
        "fluxIAE_Formations" formations
    group by
        formations.formation_id_ctr
)
select
    salarie.salarie_id as id_salarie,
    salarie.salarie_rci_libelle as civilite,
    refmotifsort.rms_libelle as motif_sortie,
    contrat.contrat_date_embauche as date_embauche,
    contrat.contrat_id_ctr as contrat_id,
    formations.nb_formations as nb_formations,
    formations.nb_jours_formation as nb_jours_formation,
    formations.nb_heures_formation as nb_heures_formation,
    formations.nb_min_formation as nb_min_formation,
    case when contrat.contrat_salarie_rqth then
        'Oui'
    else
        'Non'
    end as rqth,
    case when contrat.contrat_salarie_aah then
        'Oui'
    else
        'Non'
    end as aah,
    case when contrat.contrat_salarie_oeth then
        'Oui'
    else
        'Non'
    end as oeth,
    case when contrat.contrat_salarie_ass then
        'Oui'
    else
        'Non'
    end as ass,
    case when contrat.contrat_salarie_is_zrr then
        'Oui'
    else
        'Non'
    end as zrr,
    case when contrat.contrat_salarie_aide_sociale then
        'Oui'
    else
        'Non'
    end as aide_soc
from
    "fluxIAE_Salarie" salarie
    -- pour récupération du contrat réalisé pour un salarié
    -- une ligne par salarié et par contrat
    -- pour les EITI il y a un nb très négligeable de salariés ayant eu plusieurs contrats (5)
    left join "fluxIAE_ContratMission" contrat on contrat_id_pph = salarie_id
    -- pour récupération du libellé du motif de sortie
    left join "fluxIAE_RefMotifSort" refmotifsort on rms_id = contrat_motif_sortie_id
    left join formations_par_contrat formations on formation_id_ctr = contrat_id_ctr
where
    contrat_mesure_disp_code = 'EITI_DC';