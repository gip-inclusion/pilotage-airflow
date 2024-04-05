select
    {{ pilo_star(source('oneshot', 'ft_iae_nord'), relation_alias='ft') }},
    {{ pilo_star(ref('candidatures_echelle_locale'), relation_alias='cd') }}
from {{ source('oneshot', 'ft_iae_nord') }} as ft
left join {{ ref('candidats') }} as c
    on ft.nir_hash = c.hash_nir
left join {{ ref('candidatures_echelle_locale') }} as cd
    on c.id = cd.id_candidat
