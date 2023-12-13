{{ config(
    materialized='incremental',
    unique_key='emi_dsm_id',
    indexes=[
      {'columns': ['emi_dsm_id'], 'unique' : True},
      {'columns': ['emi_pph_id'], 'unique' : False},
      {'columns': ['emi_afi_id'], 'unique' : False},
      {'columns': ['emi_ctr_id'], 'unique' : False},
      {'columns': ['emi_motif_sortie_id'], 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_EtatMensuelIndiv'), relation_alias='emi') }},
    to_date(concat('01-', emi.emi_sme_mois, '-', emi.emi_sme_annee), 'DD-MM-YYYY') as date_emi
from
    {{ source('fluxIAE', 'fluxIAE_EtatMensuelIndiv') }} as emi
where
    emi.emi_sme_annee in
    (
        date_part('year', current_date),
        date_part('year', current_date) - 1,
        date_part('year', current_date) - 2
    )
{% if is_incremental() %}
    and {{ to_timestamp('emi.emi_date_modification') }} > (select max({{ to_timestamp('emi_date_modification') }}) from {{ this }})
{% endif %}
