select
    {{ pilo_star(ref('candidats'),  relation_alias="c") }},
    cd.date_embauche,
    cd.date_candidature,
    cd."état",
    cd.id_structure,
    cd.origine,
    case
        when pass.id is null then 'Non'
        else 'Oui'
    end as "Pass en cours de validité"
from {{ ref('candidats') }} as c
left join {{ source('emplois', 'candidatures') }} as cd
    on c.id = cd.id_candidat
left join {{ ref('pass_agrements_valides') }} as pass
    on pass.id_candidat = c.id
where (total_candidatures > 0 or diagnostic_valide = 1) and (date_diagnostic is null or date_diagnostic <= date_candidature)
