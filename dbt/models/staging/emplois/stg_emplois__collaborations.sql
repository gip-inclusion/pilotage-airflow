select
    id_utilisateur,
    administrateur,
    id_structure,
    id_organisation,
    id_institution,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'collaborations') }}
