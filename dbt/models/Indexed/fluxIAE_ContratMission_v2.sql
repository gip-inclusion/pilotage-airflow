{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['contrat_id_ctr'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_ContratMission'), relation_alias='ctr') }}
from
    {{ source('fluxIAE', 'fluxIAE_ContratMission') }}
