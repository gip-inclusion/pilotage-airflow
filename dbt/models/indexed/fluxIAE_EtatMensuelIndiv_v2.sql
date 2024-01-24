{{ config(
    indexes=[
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
    emi.emi_sme_annee >= 2021
