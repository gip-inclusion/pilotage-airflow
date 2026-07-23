select
    id,
    kind
from {{ source('raw_marche', 'users_user') }}
