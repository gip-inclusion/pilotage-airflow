with actual_counts as (
    select
        is_logged_imer,
        count(*) as row_count
    from {{ ref('fct_dora__imer') }}
    where origin_source in ('dora', 'dora-data-inclusion')
    group by is_logged_imer
),

expected_counts as (
    select
        is_logged as is_logged_imer,
        count(*)  as row_count
    from {{ ref('int_dora__imer') }}
    group by is_logged
),

compared as (
    select
        coalesce(actual_counts.is_logged_imer, expected_counts.is_logged_imer) as is_logged_imer,
        coalesce(actual_counts.row_count, 0)                                   as actual_count,
        coalesce(expected_counts.row_count, 0)                                 as expected_count
    from actual_counts
    full outer join expected_counts
        on actual_counts.is_logged_imer = expected_counts.is_logged_imer
)

select *
from compared
where actual_count != expected_count
