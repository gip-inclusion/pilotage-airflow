select
    id,
    siae_id,
    updated_at::timestamp as updated_at
from {{ source('raw_marche', 'siaes_siaeoffer') }}
