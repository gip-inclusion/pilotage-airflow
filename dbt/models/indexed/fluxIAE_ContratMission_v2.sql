{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['contrat_id_ctr'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_ContratMission'), except=['contrat_date_embauche', "contrat_date_fin_contrat"]) }},
    to_date(ctr.contrat_date_embauche, 'DD/MM/YYYY')    as contrat_date_embauche,
    to_date(ctr.contrat_date_fin_contrat, 'DD/MM/YYYY') as contrat_date_fin_contrat,
    rnf.rnf_libelle_niveau_form_empl                    as niveau_de_formation,
    rdp.rdp_lib_duree_pe                                as duree_inscription_ft
from
    {{ source('fluxIAE', 'fluxIAE_ContratMission') }} as ctr
left join {{ source('fluxIAE', 'fluxIAE_RefNiveauFormation') }} as rnf
    on ctr.contrat_niveau_de_formation_id = rnf.rnf_id
left join {{ source('fluxIAE', 'fluxIAE_RefDureePoleEmploi') }} as rdp
    on ctr.contrat_salarie_pe_duree_id = rdp.rdp_id
