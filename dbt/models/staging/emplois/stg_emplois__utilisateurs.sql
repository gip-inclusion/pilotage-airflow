select
    id,
    uid,
    email,
    type,
    prenom,
    nom,
    "dernière_connexion"        as derniere_connexion,
    date_inscription,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'utilisateurs_v0') }}
