{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['emi_pph_id'], 'unique' : False},
    ]
 ) }}

select
    emi_pph_id,
    count(emi_pph_id) as nombre_mois_travailles
from
    {{ ref('fluxIAE_EtatMensuelIndiv_v2') }}
where emi_nb_heures_travail > 0
group by emi_pph_id
