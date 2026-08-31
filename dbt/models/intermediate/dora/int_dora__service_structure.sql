with services as (
    select * from {{ ref("stg_dora__service") }}
),

structures as (
    select * from {{ ref("int_dora__structure_non_obsolete") }}
),

final as (
    select
        {{ dbt_utils.star(relation_alias='services', from=ref("stg_dora__service"), prefix='service_') }},
        {{ dbt_utils.star(relation_alias='structures', from=ref("int_dora__structure_non_obsolete"), prefix='structure_') }}
    from services
    inner join structures on services.structure_id = structures.id
)

select * from final
