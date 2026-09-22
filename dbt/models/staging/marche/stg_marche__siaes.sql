select
    id,
    department,
    admin_email
from {{ source('raw_marche', 'siaes_siae') }}
