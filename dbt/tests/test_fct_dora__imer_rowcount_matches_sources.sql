with counts as (
    select
        (select count(*) from {{ ref('fct_dora__imer') }}) as actual_count,
        (
            (select count(*) from {{ ref('stg_emplois__imer') }})
            + (select count(*) from {{ ref('int_dora__imer') }})
        )                                                           as expected_count
)

select *
from counts
where actual_count != expected_count
