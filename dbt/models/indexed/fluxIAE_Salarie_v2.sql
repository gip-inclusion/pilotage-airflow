{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['salarie_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select distinct
    {{ pilo_star(source('fluxIAE', 'fluxIAE_Salarie'), except=["hash_numéro_pass_iae"]) }}
from
    {{ source('fluxIAE', 'fluxIAE_Salarie') }}
