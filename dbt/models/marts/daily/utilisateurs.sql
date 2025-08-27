select
    {{ pilo_star(source('emplois', 'utilisateurs_v0'), relation_alias="utilisateurs" ) }},
    collaborations.id_structure,
    collaborations.id_organisation,
    collaborations.id_institution
from {{ source('emplois', 'utilisateurs_v0') }} as utilisateurs
left join {{ source('emplois', 'collaborations') }} as collaborations
    on utilisateurs.id = collaborations.id_utilisateur
