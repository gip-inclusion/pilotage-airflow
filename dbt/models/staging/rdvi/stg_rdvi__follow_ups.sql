select
    id,
    user_id,
    motif_category_id,
    status,
    closed_at::timestamp as closed_at
from {{ source('rdv_insertion', 'follow_ups') }}
