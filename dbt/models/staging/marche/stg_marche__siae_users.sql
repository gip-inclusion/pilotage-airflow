select
    id,
    siae_id,
    user_id
from {{ source('raw_marche', 'siaes_siaeuser') }}
