{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['sorties_pph_id'], 'type' : 'btree', 'unique' : False},
      {'columns': ['sorties_afi_id'], 'type' : 'btree', 'unique' : False},
      {'columns': ['sorties_ctr_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select distinct
    emi.emi_pph_id as sorties_pph_id,
    emi.emi_afi_id as sorties_afi_id,
    emi.emi_ctr_id as sorties_ctr_id,
    rcs.rcs_libelle,
    rms.rms_libelle
from {{ ref("fluxIAE_EtatMensuelIndiv_v2") }} as emi
left join {{ ref("fluxIAE_ContratMission_v2") }} as ctr
    on emi.emi_ctr_id = ctr.contrat_id_ctr
left join {{ ref("fluxIAE_RefMotifSort_v2") }} as rms
    on emi.emi_motif_sortie_id = rms.rms_id
left join {{ ref("fluxIAE_RefCategorieSort_v2") }} as rcs
    on rms.rcs_id = rcs.rcs_id
where
    rcs.rcs_libelle is not null
    and rcs.rcs_libelle != 'Retrait des sorties constatées'
