with principales as (

    select
        source,
        id,
        doublons
    from {{ ref('int_di__structures') }}
    where est_structure_principale

),

structure_source_mapping as (

    select
        source as deduplicated_source,
        id     as deduplicated_source_structure_id,
        source,
        id     as source_structure_id
    from principales

    union all

    select
        principales.source                as deduplicated_source,
        principales.id                    as deduplicated_source_structure_id,
        doublon.value::jsonb ->> 'source' as source,
        doublon.value::jsonb ->> 'id'     as source_structure_id
    from principales
    cross join lateral unnest(principales.doublons) as doublon (value)

)

select
    {{ dbt_utils.generate_surrogate_key(['deduplicated_source', 'deduplicated_source_structure_id']) }} as structure_id,
    source,
    source_structure_id
from structure_source_mapping
