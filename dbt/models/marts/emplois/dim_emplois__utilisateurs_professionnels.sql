select
    id,
    uid,
    email,
    type,
    prenom,
    nom,
    derniere_connexion,
    date_inscription,
    date_mise_a_jour_metabase,
    avec_telephone
from {{ ref('stg_emplois__utilisateurs') }}
