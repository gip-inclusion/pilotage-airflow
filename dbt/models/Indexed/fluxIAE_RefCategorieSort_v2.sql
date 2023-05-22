{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['rcs_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_RefCategorieSort'), relation_alias='rms') }}
from
    {{ source('fluxIAE', 'fluxIAE_RefCategorieSort') }}
