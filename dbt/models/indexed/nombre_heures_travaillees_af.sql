{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['emi_pph_id','af_numero_annexe_financiere'], 'unique' : False},
    ]
 ) }}

select
    emi_pph_id,
    af_numero_annexe_financiere,
    sum(emi_nb_heures_travail) as total_heures_travaillees_derniere_af
from {{ ref('sorties_v2') }}
where emi_sme_annee = annee_sortie_definitive
group by
    emi_pph_id,
    af_numero_annexe_financiere
