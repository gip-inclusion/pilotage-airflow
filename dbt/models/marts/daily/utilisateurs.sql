select
    {{ pilo_star(source('raw_emplois', 'utilisateurs_v0'), relation_alias="utilisateurs" ) }},
    collaborations.id_structure,
    collaborations.id_organisation,
    collaborations.id_institution,
    collaborations.administrateur
from {{ source('raw_emplois', 'utilisateurs_v0') }} as utilisateurs
left join {{ source('raw_emplois', 'collaborations') }} as collaborations
    on utilisateurs.id = collaborations.id_utilisateur
