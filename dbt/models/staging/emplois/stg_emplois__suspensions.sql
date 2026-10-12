select
    id                          as id_suspension,
    "id_pass_agrément"          as id_pass_iae,
    "date_début"                as date_debut,
    date_fin,
    {{ clean_text('motif') }} as motif,
    en_cours = 1                as en_cours,
    "date_de_création"          as date_creation,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'suspensions_v0') }}
