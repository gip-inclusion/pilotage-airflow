with src_events as (
    select * from {{ source('dora', 'stats_mobilisationevent') }}
),

src_di_events as (
    select * from {{ source('dora', 'stats_dimobilisationevent') }}
),

events as (
    select
        'dora-' || src.id              as id,
        src.path,
        src.date,
        src.anonymous_user_hash,
        src.is_logged,
        src.is_staff,
        src.is_manager,
        src.is_an_admin,
        src.is_structure_admin,
        src.is_structure_member,
        src.user_kind,
        src.structure_department,
        cast(src.structure_id as text) as structure_id,
        src.user_id,
        false                          as is_di

    from src_events as src

),

di_events as (
    select
        'di-' || src.id                as id,
        src.path,
        src.date,
        src.anonymous_user_hash,
        src.is_logged,
        src.is_staff,
        src.is_manager,
        src.is_an_admin,
        false
            as is_structure_admin,
        false
            as is_structure_member,
        src.user_kind,
        src.structure_department,
        cast(src.structure_id as text) as structure_id,
        src.user_id,
        true                           as is_di
    from src_di_events as src
),

final as (
    select * from events
    union
    select *
    from di_events
    where date >= '2024-01-01'
)

select * from final
