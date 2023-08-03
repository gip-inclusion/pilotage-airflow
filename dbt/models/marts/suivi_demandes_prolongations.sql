select
    {{ dbt_utils.star(source('emplois','demandes_de_prolongation'), relation_alias='prolong') }},
    o.nom                                               as nom_prescripteur,
    o.type_complet                                      as type_prescripteur,
    o."nom_département"                                 as "département_prescripteur",
    o."région"                                          as "région_prescripteur",
    s.nom                                               as nom_structure,
    s.nom_complet                                       as nom_complet_structure,
    s.type                                              as type_structure,
    s."nom_département"                                 as "département_structure",
    s."région"                                          as "région_structure",
    /* delai_traitement is in days*/
    (prolong.date_traitement - prolong.date_de_demande) as delai_traitement
from {{ source('emplois', 'demandes_de_prolongation') }} as prolong
left join {{ source('emplois', 'organisations') }} as o
    on prolong.id_organisation_prescripteur = o.id
left join {{ source('emplois', 'structures') }} as s
    on prolong."id_structure_déclarante" = s.id
