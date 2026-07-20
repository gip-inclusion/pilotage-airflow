with orientations as (
    select * from {{ source('dora', 'orientations_orientation') }}
),

final as (
    select * from orientations
    -- date discutée avec Chloé pour limiter la taille de la table
    where cast(creation_date as DATE) >= '2024-01-01'
)

select * from final
