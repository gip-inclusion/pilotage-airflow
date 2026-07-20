with src as (
    select * from {{ source('dora', 'services_service') }}
    where is_model is false
),

services as (
    select
        {{ pilo_star(source('dora', 'services_service'), relation_alias='src', except=['geom']) }},
        'dora--' || src.id               as id_jointure_di,
        st_y(cast(src.geom as geometry)) as latitude,
        st_x(cast(src.geom as geometry)) as longitude,
        case
            when src.status != 'PUBLISHED' then false
            when
                src.update_frequency = 'tous-les-mois'
                and src.modification_date + interval '1 month' <= now() then true
            when
                src.update_frequency = 'tous-les-3-mois'
                and src.modification_date + interval '3 months' <= now() then true
            when
                src.update_frequency = 'tous-les-6-mois'
                and src.modification_date + interval '6 months' <= now() then true
            when
                src.update_frequency = 'tous-les-12-mois'
                and src.modification_date + interval '12 months' <= now() then true
            when
                src.update_frequency = 'tous-les-16-mois'
                and src.modification_date + interval '16 months' <= now() then true
            else false
        end                              as update_needed
    from src
),

final as (
    select
        services.*,
        concat('https://dora.inclusion.beta.gouv.fr/services/', services.slug) as dora_url
    from services
)

select * from final
