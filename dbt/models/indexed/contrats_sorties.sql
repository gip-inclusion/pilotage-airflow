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
    /* 1 day = 0.0328767 months */
    (rcrt.date_sortie_definitive::DATE - rcrt.date_recrutement::DATE) * (0.0328767) as duree_en_mois,
    case
        when ctr.contrat_salarie_rsa in ('OUI-M', 'OUI-NM') then 'OUI'
        else 'NON'
    end                                                                             as salarie_brsa,
    case
        when sal.salarie_rci_libelle = 'MME' then 'Femme'
        when sal.salarie_rci_libelle = 'M.' then 'Homme'
        else 'Non renseigné'
    end                                                                             as genre_salarie
from {{ ref("fluxIAE_EtatMensuelIndiv_v2") }} as emi
left join {{ ref("fluxIAE_ContratMission_v2" ) }} as ctr
    on emi.emi_ctr_id = ctr.contrat_id_ctr
left join {{ ref("fluxIAE_Salarie_v2" ) }} as sal
    on emi.emi_pph_id = sal.salarie_id
left join {{ ref("stg_recrutements" ) }} as rcrt
    on
        emi.emi_pph_id = rcrt.contrat_id_pph
        and emi.emi_ctr_id = rcrt.id_derniere_reconduction
where
    emi.emi_sme_annee >= 2021
    and (ctr.contrat_date_sortie_definitive is not null or rcrt.date_sortie_definitive is not null)
    and ctr.contrat_motif_sortie_id is not null
