select
    id_candidature,
    id_fiche_de_poste,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('emplois', 'fiches_de_poste_par_candidature') }}
