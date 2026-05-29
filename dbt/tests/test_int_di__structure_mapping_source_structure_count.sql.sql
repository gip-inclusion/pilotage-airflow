with mapping as (

    select
        count(distinct source_structure_id) as source_structure_id_count
    from {{ ref('int_di__structure_mapping') }}

),

structures as (

    select
        count(distinct id) as structure_id_count
    from {{ ref('stg_di__structures') }}

)

select
    mapping.source_structure_id_count,
    structures.structure_id_count
from mapping
cross join structures
where mapping.source_structure_id_count != structures.structure_id_count
