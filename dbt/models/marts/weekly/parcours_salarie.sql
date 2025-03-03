select
    pass.hash_nir,
    {{ pilo_star(ref('stg_parcours_salarie_info_pass'), except=["hash_nir"]) }},
    {{ pilo_star(ref('stg_parcours_salarie_info_contrats'), except=["hash_nir"]) }}
from {{ ref('stg_parcours_salarie_info_pass') }} as pass
left join {{ ref('stg_parcours_salarie_info_contrats') }} as ctr
    on pass.hash_nir = ctr.hash_nir
