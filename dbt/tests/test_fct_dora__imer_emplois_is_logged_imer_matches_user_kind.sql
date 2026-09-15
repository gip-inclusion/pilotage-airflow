select *
from {{ ref('fct_dora__imer') }}
where
    origin_source = 'emplois'
    and (
        user_kind is null
        or (user_kind = 'emplois_anonymous' and is_logged_imer is not false)
        or (user_kind != 'emplois_anonymous' and is_logged_imer is not true)
    )
