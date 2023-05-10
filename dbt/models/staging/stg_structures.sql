select
    s.id          as id,
    s.siret       as siret,
    s.active      as active,
    s.ville       as ville,
    s.nom_complet as "nom_structure_complet"
from
    {{ source('emplois', 'structures') }} as s
