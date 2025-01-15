{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['emi_pph_id'], 'type' : 'btree', 'unique' : False},
      {'columns': ['emi_ctr_id'], 'type' : 'btree', 'unique' : False},
      {'columns': ['emi_afi_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(ref('etat_mensuel_heures_travaillees_sorties'), relation_alias='ems') }},
    {{ pilo_star(ref('motif_sorties_salaries'), relation_alias='sorties') }}
from {{ ref("etat_mensuel_heures_travaillees_sorties") }} as ems
left join {{ ref("motif_sorties_salaries") }} as sorties
    on
        ems.emi_pph_id = sorties.sorties_pph_id
        and ems.emi_afi_id = sorties.sorties_afi_id
        and ems.emi_ctr_id = sorties.sorties_ctr_id
where ems.nombre_heures_travaillees >= 150
