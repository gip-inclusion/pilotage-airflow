select
    id,
    type,
    nom,
    "département"               as departement,
    "nom_département"           as nom_departement,
    "région"                    as region,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'institutions') }}
