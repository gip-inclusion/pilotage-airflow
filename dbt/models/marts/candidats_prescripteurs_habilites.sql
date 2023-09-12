select
    {{ pilo_star(source('emplois', 'candidats'), relation_alias="cdd") }},
    cdd."total_critères_niveau_1" + cdd."total_critères_niveau_2" as "total_critères",
    org.type_prescripteur                                         as "type_prescripteur"
from
    {{ source('emplois', 'candidats') }} as cdd
left join
    {{ ref('stg_organisations') }} as org
    on cdd.id_auteur_diagnostic_prescripteur = org.id
where
    cdd.type_inscription = 'par prescripteur'
