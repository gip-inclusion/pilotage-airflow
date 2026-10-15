{{ config(tags=['dora']) }}

with expected_ids as (
    select
        'dora'   as origin_source,
        id::text as id
    from {{ ref('stg_dora__orientation') }}
    where prescriber_id is not null

    union all

    select
        'emplois' as origin_source,
        id::text  as id
    from {{ ref('stg_emplois__orientations') }}
),

expected as (
    select
        origin_source,
        id,
        count(*) as row_count
    from expected_ids
    group by origin_source, id
),

actual as (
    select
        origin_source,
        id,
        count(*) as row_count
    from {{ ref('fct_dora__orientations') }}
    group by origin_source, id
)

select
    expected.row_count                                     as expected_count,
    actual.row_count                                       as actual_count,
    coalesce(expected.origin_source, actual.origin_source) as origin_source,
    coalesce(expected.id, actual.id)                       as id
from expected
full outer join actual
    on expected.origin_source = actual.origin_source and expected.id = actual.id
where expected.row_count is distinct from actual.row_count
