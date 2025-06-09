select
    pass.hash_nir,
    {{ pilo_star(ref('stg_parcours_salarie_info_pass'), except=["hash_nir"]) }},
    {{ pilo_star(ref('stg_parcours_salarie_info_contrats'), except=["hash_nir"]) }},
    carac_benef.genre_salarie,
    carac_benef.contrat_salarie_ass,
    carac_benef.contrat_salarie_aah,
    carac_benef.contrat_salarie_rqth,
    carac_benef.salarie_adr_is_zrr,
    carac_benef.duree_inscription_ft,
    carac_benef.beneficiaires_du_rsa,
    carac_benef.salarie_qpv,
    carac_benef.tranche_age_beneficiaires,
    carac_benef.niveau_de_formation
from {{ ref('stg_parcours_salarie_info_pass') }} as pass
left join {{ ref('stg_parcours_salarie_info_contrats') }} as ctr
    on pass.hash_nir = ctr.hash_nir
left join {{ ref('eph_caracteristiques_beneficiaires_per_hash_nir') }} as carac_benef
    on pass.hash_nir = carac_benef.hash_nir
