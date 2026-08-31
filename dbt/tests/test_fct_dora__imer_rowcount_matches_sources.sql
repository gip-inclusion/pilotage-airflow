with actual_count as (
    select count(*) as row_count
    from {{ ref('fct_dora__imer') }}
),

expected_counts as (
    select count(*) as row_count
    from {{ ref('stg_emplois__imer') }}

    union all

    select count(*) as row_count
    from {{ ref('int_dora__imer') }}
),

counts as (
    select
        actual_count.row_count         as actual_count,
        sum(expected_counts.row_count) as expected_count
    from actual_count
    cross join expected_counts
    group by actual_count.row_count
)

select *
from counts
where actual_count != expected_count
