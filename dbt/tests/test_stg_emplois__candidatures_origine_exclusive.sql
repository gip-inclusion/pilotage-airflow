select
    id_candidature,
    id_organisation_prescriptrice,
    id_structure_origine
from {{ ref('stg_emplois__candidatures') }}
where
    id_organisation_prescriptrice is not null
    and id_structure_origine is not null
