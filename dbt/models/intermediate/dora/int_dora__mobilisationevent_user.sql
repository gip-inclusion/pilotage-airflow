with events as (
    select * from {{ ref('stg_dora__mobilisationevent') }}
),

users as (
    select * from {{ ref('int_dora__active_user') }}
),

final as (
    select
        events.id as mobilisation_id,
        events.user_id
    from events
    inner join users
        on events.user_id = users.id
)

select * from final
