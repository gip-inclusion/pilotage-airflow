select
    id                          as id_prolongation,
    "id_pass_agrément"          as id_pass_iae,
    "date_début"                as date_debut,
    date_fin,
    {{ clean_text('motif') }} as motif,
    "id_utilisateur_déclarant"  as id_utilisateur_declarant,
    "id_structure_déclarante"   as id_structure_declarante,
    id_organisation_prescripteur,
    id_utilisateur_prescripteur,
    "date_de_création"          as date_creation,
    id_demande_de_prolongation,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'prolongations') }}
