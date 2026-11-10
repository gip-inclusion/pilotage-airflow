with dim_structures as (

    select *
    from {{ ref('dim_data_inclusion__structures') }}

),

deduplicated_structures as (

    select *
    from {{ ref('int_di__structures') }}
    where est_structure_principale

),

row_counts as (

    select
        (select count(*) from dim_structures) as dim_structures_count,
        (
            select count(distinct id)
            from deduplicated_structures
        )                                     as deduplicated_structures_count

)

select *
from row_counts
where dim_structures_count != deduplicated_structures_count
