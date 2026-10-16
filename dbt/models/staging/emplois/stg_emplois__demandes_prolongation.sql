select
    id                                 as id_demande_prolongation,
    "id_pass_agrément"                 as id_pass_iae,
    "date_début"                       as date_debut,
    date_fin,
    {{ clean_text('motif') }} as motif,
    "id_utilisateur_déclarant"         as id_utilisateur_declarant,
    "id_structure_déclarante"          as id_structure_declarante,
    id_organisation_prescripteur,
    id_utilisateur_prescripteur,
    id_prolongation,
    {{ clean_text('état') }} as etat,
    {{ clean_text('motif_de_refus') }} as motif_de_refus,
    date_de_demande,
    date_traitement,
    id_utilisateur_traitant,
    date_envoi_rappel,
    "date_mise_à_jour_metabase"        as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'demandes_de_prolongation') }}
