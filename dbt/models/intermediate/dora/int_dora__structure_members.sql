with users as (
    select * from {{ ref('stg_dora__user') }}
),

structures as (
    select * from {{ ref('int_dora__structure_non_obsolete') }}
),

member as (
    select * from {{ ref('stg_dora__structure_member') }}
),

final as (
    select
        {{ dbt_utils.star(relation_alias='users', from=ref('stg_dora__user'), prefix='user_') }},
        {{ dbt_utils.star(relation_alias='structures', from=ref('int_dora__structure_non_obsolete'), prefix='structure_') }}
    from member
    inner join structures on member.structure_id = structures.id
    inner join users on member.user_id = users.id
)

select * from final
