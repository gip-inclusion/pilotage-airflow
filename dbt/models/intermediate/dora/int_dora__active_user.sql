select *
from {{ ref('stg_dora__user') }}
where
    is_active is true
    and is_valid is true
    and is_staff is false
