select
    id,
    author_id,
    location_id,
    kind,
    status,
    siae_count,
    created_at::timestamp as created_at
from {{ source('raw_marche', 'tenders_tender') }}
