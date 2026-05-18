select distinct
    territoire_id,
    territoire_libelle,
    departement_id,
    nom_departement,
    nom_region
from {{ ref('ref_clpe_ft') }}
