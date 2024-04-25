select {{ pilo_star(ref('stg_candidats_candidatures'), relation_alias="cdd") }}
from {{ ref('stg_candidats_candidatures') }} as cdd
right join {{ ref('candidats_recherche_active') }} as cra
    on cra.id = cdd.id
