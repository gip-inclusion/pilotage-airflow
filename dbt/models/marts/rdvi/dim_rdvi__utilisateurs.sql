select
    id                                         as user_id,
    created_at,
    created_through,
    role,
    rights_opening_date,
    birth_date,
    updated_at,
    deleted_at,
    deleted_at is not null                     as is_deleted,
    nullif(trim(email), '') is not null        as has_email,
    nullif(trim(phone_number), '') is not null as has_phone_number
from {{ ref('stg_rdvi__users') }}
