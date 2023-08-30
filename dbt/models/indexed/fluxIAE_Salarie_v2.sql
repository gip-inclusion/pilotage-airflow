{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['salarie_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_Salarie')) }}
from
    {{ source('fluxIAE', 'fluxIAE_Salarie') }}
