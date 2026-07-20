with users as (
    select * from {{ ref('stg_dora__user') }}
),

structure as (
    select * from {{ ref('stg_dora__structure') }}
),

member as (
    select * from {{ source('dora', 'structures_structuremember') }}
),

final as (
    select
        {{ dbt_utils.star(relation_alias='users', from=ref('stg_dora__user'), prefix='user_') }},
        {{ dbt_utils.star(relation_alias='structure', from=ref('stg_dora__structure'), prefix='structure_') }}
    from member
    inner join structure on member.structure_id = structure.id
    inner join users on member.user_id = users.id
)

select * from final
