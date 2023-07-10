{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['contrat_id_ctr'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_ContratMission')) }}
from
    {{ source('fluxIAE', 'fluxIAE_ContratMission') }}
