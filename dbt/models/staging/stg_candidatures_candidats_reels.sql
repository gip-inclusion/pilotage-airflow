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
-- ce sont des candidats reels soit si ils ont un diagnostic valide ou au moins une candidature
-- et si la date de diagnostic n'est pas nulle, on ne garde que les candidatures après son dernier diagnostic
where (c.diagnostic_valide = 1 and (c.date_diagnostic <= cd.date_candidature or cd.date_candidature is null))
       or (cd.date_candidature is not null) -- cas des candidats non encore diagnostiqués
