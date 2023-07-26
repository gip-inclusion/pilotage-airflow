select
    {{ pilo_star(source('emplois', 'candidats'), relation_alias='candidats') }},
    coalesce(organisations."région", structures."région")                   as "région_diag",
    coalesce(organisations."nom_département", structures."nom_département") as "département_diag",
    -- for know only reliable to the year because we do not consider month for calculating the age
    -- todo : correct it to consider month also
    case
        when annee_naissance_selon_nir < extract(year from current_date) - 2000 then extract(year from current_date) - (annee_naissance_selon_nir + 2000)
        else extract(year from current_date) - (annee_naissance_selon_nir + 1900)
    end                                                                     as age_selon_nir
from
    {{ source('emplois', 'candidats') }} as candidats
left join
    {{ ref('stg_organisations') }} as organisations
    on organisations.id = candidats.id_auteur_diagnostic_prescripteur
left join
    {{ source('emplois', 'structures') }} as structures
    on structures.id = candidats.id_auteur_diagnostic_employeur
