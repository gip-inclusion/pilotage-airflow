with services as (
    select * from {{ ref("stg_dora__service") }}
),

structure as (
    select * from {{ ref("stg_dora__structure") }}
),

final as (
    select
        {{ dbt_utils.star(relation_alias='services', from=ref("stg_dora__service"), prefix='service_') }},
        {{ dbt_utils.star(relation_alias='structure', from=ref("stg_dora__structure"), prefix='structure_') }}
    from services
    inner join structure on services.structure_id = structure.id
)

select * from final
