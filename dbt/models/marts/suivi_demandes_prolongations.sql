select
    /* TODO(ypassa): change pilo_star to dbt_utils.star once Victor's PR will be merged */
    {{ pilo_star(source('emplois','demandes_de_prolongation'), relation_alias='ddp') }},
    o.nom               as nom_prescripteur,
    o.type_complet      as type_prescripteur,
    o."nom_département" as "département_prescripteur",
    o."région"          as "région_prescripteur",
    s.nom               as nom_structure,
    s.nom_complet       as nom_complet_structure,
    s.type              as type_structure,
    s."nom_département" as "département_structure",
    s."région"          as "région_structure"
from {{ source('emplois', 'demandes_de_prolongation') }} as ddp
left join {{ source('emplois', 'organisations') }} as o
    on ddp.id_organisation_prescripteur = o.id
left join {{ source('emplois', 'structures') }} as s
    on ddp."id_structure_déclarante" = s.id
