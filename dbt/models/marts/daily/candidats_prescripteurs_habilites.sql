select
    {{ pilo_star(ref('candidats'), relation_alias="cdd") }},
    org.type_prescripteur,
    cdd."total_critères_niveau_1" + cdd."total_critères_niveau_2" as "total_critères"
from
    {{ ref('candidats') }} as cdd
left join
    {{ ref('stg_organisations') }} as org
    on cdd.id_auteur_diagnostic_prescripteur = org.id
where
    cdd.type_inscription = 'par prescripteur'
