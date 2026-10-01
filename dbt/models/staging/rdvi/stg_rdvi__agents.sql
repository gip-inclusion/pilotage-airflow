select
    id,
    email,
    first_name,
    last_name,
    super_admin::boolean as super_admin,
    last_sign_in_at,
    created_at
from {{ source('rdv_insertion', 'agents') }}
