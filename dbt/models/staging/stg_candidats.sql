select
    {{ pilo_star(source('emplois', 'candidats'), relation_alias='candidats') }},
    -- for know only reliable to the year because we do not consider month for calculating the age
    -- todo : correct it to consider month also
    case
        when annee_naissance_selon_nir < extract(year from current_date) - 2000 then extract(year from current_date) - (annee_naissance_selon_nir + 2000)
        else extract(year from current_date) - (annee_naissance_selon_nir + 1900)
    end as age_selon_nir
from
    {{ source('emplois', 'candidats') }}
