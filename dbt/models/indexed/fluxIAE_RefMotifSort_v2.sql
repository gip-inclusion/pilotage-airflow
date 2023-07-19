{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['rms_id'], 'type' : 'btree', 'unique' : False},
      {'columns': ['rcs_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_RefMotifSort')) }}
from
    {{ source('fluxIAE', 'fluxIAE_RefMotifSort') }}
