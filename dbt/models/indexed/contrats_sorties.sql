{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['emi_pph_id'], 'type' : 'btree', 'unique' : False},
      {'columns': ['emi_ctr_id'], 'type' : 'btree', 'unique' : False},
      {'columns': ['emi_afi_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}


select distinct
    emi.emi_pph_id,
    emi.emi_afi_id,
    emi.emi_ctr_id,
    emi.emi_sme_annee,
    ctr.contrat_date_creation,
    ctr.contrat_date_embauche,
    ctr.contrat_date_sortie_definitive,
    ctr.contrat_date_fin_contrat,
    rcrt.date_recrutement,
    rcrt.date_sortie_definitive,
    rcrt.id_derniere_reconduction,
    ctr.contrat_duree_contrat,
    ctr.contrat_salarie_rsa,
    extract(year from to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY'))    as annee_sortie_definitive,
    extract(year from ctr.contrat_date_fin_contrat)                                 as annee_fin_contrat,
    date_trunc('year', date(extract(
        year from
        to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')
    ) || '-01-01'))                                                                 as debut_annee_fin_contrat,
    /* 0.0328767 = the value to convert days to months */
    (rcrt.date_sortie_definitive::DATE - rcrt.date_recrutement::DATE) * (0.0328767) as duree_en_mois
from {{ ref("fluxIAE_EtatMensuelIndiv_v2") }} as emi
left join {{ ref("fluxIAE_ContratMission_v2" ) }} as ctr
    on emi.emi_ctr_id = ctr.contrat_id_ctr
left join {{ ref("stg_recrutements" ) }} as rcrt
    on
        emi.emi_pph_id = rcrt.contrat_id_pph
        and emi.emi_ctr_id = rcrt.id_derniere_reconduction
where
    emi.emi_sme_annee >= 2021
    and (ctr.contrat_date_sortie_definitive is not null or rcrt.date_sortie_definitive is not null)
