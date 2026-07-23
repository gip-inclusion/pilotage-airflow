select
    id,
    department
from {{ source('raw_marche', 'siaes_siae') }}
