select
    {{ pilo_star(source('oneshot', 'ft_iae_nord')) }},
    {{ pilo_star(ref('candidats')) }}
from {{ source('oneshot', 'ft_iae_nord') }} as ft
left join {{ ref('candidats') }} as c
    on ft.nir_hash = c.hash_nir
