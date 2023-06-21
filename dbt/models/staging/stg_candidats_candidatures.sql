select
    {{ pilo_star(source('emplois', 'candidats'),  relation_alias="c") }},
    cd.date_embauche
from {{ source('emplois', 'candidats') }} as c
left join {{ source('emplois', 'candidatures') }} as cd
    on c.id = cd.id_candidat
