select
    id_candidat,
    id_structure
from {{ ref('stg_candidatures') }}
where origine = 'Employeur'
