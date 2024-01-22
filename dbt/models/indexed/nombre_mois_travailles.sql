{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['emi_pph_id'], 'unique' : False},
    ]
 ) }}

select
    m.emi_pph_id,
    h.af_numero_annexe_financiere,
    count(m.emi_pph_id) as nombre_mois_travailles
from
    {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as m
left join {{ ref('nombre_heures_travaillees_af') }} as h
    on h.emi_pph_id = m.emi_pph_id
where
    h.total_heures_travaillees_derniere_af > 0
    and m.emi_salarie_tjs_accomp = 'TRUE'
group by
    m.emi_pph_id,
    h.af_numero_annexe_financiere
