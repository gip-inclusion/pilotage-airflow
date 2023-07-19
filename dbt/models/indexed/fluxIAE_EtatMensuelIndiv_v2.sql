{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['emi_afi_id'], 'unique' : False},
      {'columns': ['emi_ctr_id'], 'unique' : False},
      {'columns': ['emi_motif_sortie_id'], 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_EtatMensuelIndiv'), relation_alias='emi') }}
from
    {{ source('fluxIAE', 'fluxIAE_EtatMensuelIndiv') }} as emi
where
    emi.emi_sme_annee in
    (
        date_part('year', current_date),
        date_part('year', current_date) - 1,
        date_part('year', current_date) - 2
    )
