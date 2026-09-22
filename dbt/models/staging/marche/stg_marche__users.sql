select
    id,
    kind,
    email,
    first_name,
    last_name,
    buyer_kind_detail,
    partner_kind,
    last_login,
    date_joined,
    company_name
from {{ source('raw_marche', 'users_user') }}
