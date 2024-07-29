select distinct
    ctr.contrat_id_pph,
    ctr.contrat_id_ctr,
    ctr.contrat_id_structure,
    ctr.contrat_mesure_disp_code,
    ctr.contrat_duree_contrat,
    ctr.contrat_date_embauche,
    ctr.contrat_date_fin_contrat,
    ctr.contrat_type_contrat,
    motif_sortie.rms_libelle                                                       as motif_sortie,
    sum(emi.emi_nb_heures_travail)                                                 as contrat_nb_heures,
    to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')                      as contrat_date_sortie_definitive,
    sum(case
        when ctr.contrat_type_contrat = 0 then 1
        else 0
    end) over (partition by ctr.contrat_id_pph order by ctr.contrat_date_embauche) as id_recrutement
from {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
left join {{ ref('fluxIAE_ContratMission_v2') }} as ctr
    on ctr.contrat_id_ctr = emi.emi_ctr_id
left join {{ ref('fluxIAE_RefMotifSort_v2') }} as motif_sortie
    on emi.emi_motif_sortie_id = motif_sortie.rms_id
group by
    ctr.contrat_id_pph,
    ctr.contrat_id_ctr,
    ctr.contrat_id_structure,
    ctr.contrat_mesure_disp_code,
    ctr.contrat_duree_contrat,
    ctr.contrat_date_embauche,
    ctr.contrat_date_fin_contrat,
    ctr.contrat_type_contrat,
    ctr.contrat_date_sortie_definitive,
    motif_sortie.rms_libelle
-- because we consider only contracts starting in 2021
having extract(year from ctr.contrat_date_embauche) >= 2021
