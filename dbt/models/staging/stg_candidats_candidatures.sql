select
    {{ pilo_star(ref('stg_candidats'),  relation_alias="c") }},
    cd.date_embauche
from {{ ref('stg_candidats') }} as c
left join {{ source('emplois', 'candidatures') }} as cd
    on c.id = cd.id_candidat
