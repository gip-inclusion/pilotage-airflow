select
    {{ pilo_star(source('emplois', 'candidats_v0'), relation_alias='candidats') }},
    grp_strct.groupe                                                        as categorie_structure,
    coalesce(organisations."région", structures."région")                   as "région_diag",
    coalesce(organisations."nom_département", structures."nom_département") as "département_diag",
    -- for know only reliable to the year because we do not consider month for calculating the age
    -- todo : correct it to consider month also
    case
        when candidats.annee_naissance_selon_nir < extract(year from current_date) - 2000
            then extract(year from current_date) - (candidats.annee_naissance_selon_nir + 2000)
        else extract(year from current_date) - (candidats.annee_naissance_selon_nir + 1900)
    end                                                                     as age_selon_nir
from
    {{ source('emplois', 'candidats_v0') }} as candidats
left join
    {{ ref('stg_organisations') }} as organisations
    on organisations.id = candidats.id_auteur_diagnostic_prescripteur
left join
    {{ ref('structures') }} as structures
    on structures.id = candidats.id_auteur_diagnostic_employeur
left join
    {{ ref('groupes_structures') }} as grp_strct
    on grp_strct.structure = candidats."type_structure_dernière_embauche"
