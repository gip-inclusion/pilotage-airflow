with structures as (
    select * from {{ source('dora', 'structures_structure') }}
),

final as (
    select
        structures.*,
        'dora--' || structures.id                                             as id_jointure_di,
        concat('https://dora.inclusion.gouv.fr/structures/', structures.slug) as dora_url
    from structures
)

select * from final
where not is_obsolete
