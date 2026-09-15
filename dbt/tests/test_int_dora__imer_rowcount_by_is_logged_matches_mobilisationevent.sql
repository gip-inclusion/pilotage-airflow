with actual_counts as (
    select
        is_logged,
        count(*) as row_count
    from {{ ref('int_dora__imer') }}
    group by is_logged
),

expected_counts as (
    select
        is_logged,
        count(*) as row_count
    from {{ ref('stg_dora__mobilisationevent') }}
    group by is_logged
),

compared as (
    select
        coalesce(actual_counts.is_logged, expected_counts.is_logged) as is_logged,
        coalesce(actual_counts.row_count, 0)                         as actual_count,
        coalesce(expected_counts.row_count, 0)                       as expected_count
    from actual_counts
    full outer join expected_counts
        on actual_counts.is_logged = expected_counts.is_logged
)

select *
from compared
where actual_count != expected_count
