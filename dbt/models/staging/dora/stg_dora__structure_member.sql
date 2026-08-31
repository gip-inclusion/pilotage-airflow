with src as (
    select * from {{ source('dora', 'structures_structuremember') }}
),

final as (
    select
        id,
        structure_id,
        user_id,
        is_admin,
        creation_date
    from src
)

select * from final
