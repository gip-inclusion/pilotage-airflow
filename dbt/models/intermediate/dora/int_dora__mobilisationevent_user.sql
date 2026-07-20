with users as (
    select * from {{ ref('stg_dora__user') }}
),

events as (
    select * from {{ ref('stg_dora__mobilisationevent') }}
),

final as (
    select
        {{ dbt_utils.star(relation_alias='events', from=ref('stg_dora__mobilisationevent'), prefix='event_') }},
        {{ dbt_utils.star(relation_alias='users', from=ref('stg_dora__user'), prefix='user_') }}
    from events
    inner join users on events.user_id = users.id
)

select * from final
