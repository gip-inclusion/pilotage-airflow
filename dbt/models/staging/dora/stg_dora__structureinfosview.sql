with src as (
    select * from {{ source('dora', 'stats_structureinfosview') }}
),

final as (
    select
        cast(src.id as text)                       as id,
        src.path,
        src.date,
        src.anonymous_user_hash,
        src.is_logged,
        src.is_staff,
        src.is_manager,
        src.is_an_admin,
        src.is_structure_admin,
        src.is_structure_member,
        src.structure_department,
        src.structure_city_code,
        src.structure_source                       as raw_structure_source,
        cast(src.structure_id as text)             as structure_id,
        src.user_id,
        nullif(src.user_kind, '')                  as user_kind,
        'dora--' || cast(src.structure_id as text) as structure_id_di_source
    from src
)

select * from final
