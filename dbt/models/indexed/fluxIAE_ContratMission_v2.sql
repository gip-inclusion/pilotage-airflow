{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['contrat_id_ctr'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_ContratMission'), except=['contrat_date_embauche', "contrat_date_fin_contrat"]) }},
    to_date(contrat_date_embauche, 'DD/MM/YYYY')    as contrat_date_embauche,
    to_date(contrat_date_fin_contrat, 'DD/MM/YYYY') as contrat_date_fin_contrat
from
    {{ source('fluxIAE', 'fluxIAE_ContratMission') }}
