with actual_counts as (
    select
        kind,
        count(*) as row_count
    from {{ ref('int_dora__imer') }}
    group by kind
),

expected_counts as (
    select
        'mobilisation' as kind,
        count(*)       as row_count
    from {{ ref('stg_dora__mobilisationevent') }}

    union all

    select
        'structure_contact' as kind,
        count(*)            as row_count
    from {{ ref('stg_dora__structureinfosview') }}
    where
        is_staff is false
        and is_structure_member is false
        and is_structure_admin is false
),

compared as (
    select
        coalesce(actual_counts.kind, expected_counts.kind) as kind,
        coalesce(actual_counts.row_count, 0)               as actual_count,
        coalesce(expected_counts.row_count, 0)             as expected_count
    from actual_counts
    full outer join expected_counts
        on actual_counts.kind = expected_counts.kind
)

select *
from compared
where actual_count != expected_count
