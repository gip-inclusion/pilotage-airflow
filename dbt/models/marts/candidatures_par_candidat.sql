select
    candidats.id,
    count(*)                                 as nb_candidatures,
    array_agg(candidatures.date_candidature) as date_candidature,
    array_agg(candidatures.id_structure)     as id_structure,
    array_agg(candidatures.type_structure)   as type_structure,
    array_agg(candidatures.nom_structure)    as nom_structure,
    array_agg(candidatures."état")
    as etat_candidature
from {{ source('emplois', 'candidats') }} as candidats
left join {{ ref('candidatures_echelle_locale') }} as candidatures
    on candidats.id = candidatures.id_candidat
group by candidats.id
