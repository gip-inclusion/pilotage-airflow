with deduplicated_structures as (

    select *
    from {{ ref('stg_di__structures_deduplicated') }}

),

kept_structures as (

    select
        source as deduplicated_source,
        id     as deduplicated_source_structure_id,
        source,
        id     as source_structure_id
    from deduplicated_structures

),

duplicated_structures as (

    select
        deduplicated_structures.source    as deduplicated_source,
        deduplicated_structures.id        as deduplicated_source_structure_id,
        doublon.value::jsonb ->> 'source' as source,
        doublon.value::jsonb ->> 'id'     as source_structure_id
    from deduplicated_structures
    cross join lateral unnest(deduplicated_structures.doublons) as doublon (value)

),

structure_source_mapping as (

    select *
    from kept_structures

    union all

    select *
    from duplicated_structures

),

structure_ids as (

    select distinct
        deduplicated_source,
        deduplicated_source_structure_id,
        {{ dbt_utils.generate_surrogate_key([
            'deduplicated_source',
            'deduplicated_source_structure_id'
        ]) }} as structure_id
    from structure_source_mapping

)

select
    structure_ids.structure_id,
    structure_source_mapping.source,
    structure_source_mapping.source_structure_id
from structure_source_mapping
left join structure_ids
    on
        structure_source_mapping.deduplicated_source
        = structure_ids.deduplicated_source
        and structure_source_mapping.deduplicated_source_structure_id
        = structure_ids.deduplicated_source_structure_id
