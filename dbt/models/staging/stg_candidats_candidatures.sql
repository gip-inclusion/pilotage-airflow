select
    {{ pilo_star(ref('candidats'),  relation_alias="c") }},
    cd.id as id_candidature,
    cd.date_embauche,
    cd.date_candidature,
    cd."état",
    cd.type_structure,
    cd.id_structure,
    cd.origine
from {{ ref('candidats') }} as c
left join {{ ref('candidatures_echelle_locale') }} as cd
    on c.id = cd.id_candidat
