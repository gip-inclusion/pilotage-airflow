select
    id,
    code_rome,
    nom_rome,
    recrutement_ouvert,
    type_contrat,
    id_employeur,
    type_employeur,
    siret_employeur,
    nom_employeur,
    mises_a_jour_champs,
    "département_employeur"      as departement_employeur,
    "nom_département_employeur"  as nom_departement_employeur,
    "région_employeur"           as region_employeur,
    total_candidatures,
    "date_création"              as date_creation,
    "date_dernière_modification" as date_derniere_modification,
    "date_mise_à_jour_metabase"  as date_mise_a_jour_metabase
from {{ source('emplois', 'fiches_de_poste') }}
