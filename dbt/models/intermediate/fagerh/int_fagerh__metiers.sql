with source as (

    select
        uuid,
        coalesce(nullif(metiers_json::text, ''), '[]')::jsonb as metiers_json
    from {{ ref('stg_fagerh__reponses') }}

),

unnested as (

    select
        source.uuid,
        metier.metier_item
    from source
    cross join lateral jsonb_array_elements(source.metiers_json) as metier (metier_item)

),

cleaned as (

    select
        uuid,

        nullif(replace(trim(metier_item ->> 'etpCdi'), ',', '.'), '')::numeric as etp_cdi,
        nullif(replace(trim(metier_item ->> 'etpCdd'), ',', '.'), '')::numeric as etp_cdd,
        nullif(replace(trim(metier_item ->> 'etp'), ',', '.'), '')::numeric    as etp_total,

        nullif(trim(metier_item ->> 'metier'), '')                             as metier,

        case
            when lower(trim(metier_item ->> 'mode')) = 'interne' then 'interne'
            when lower(trim(metier_item ->> 'mode')) = 'externe' then 'externe'
        end                                                                    as employment_mode

    from unnested

),

final as (

    select
        cleaned.uuid,
        cleaned.metier,
        metier_mapping.metier_family,
        metier_mapping.metier_family_label,
        cleaned.employment_mode,
        cleaned.etp_cdi,
        cleaned.etp_cdd,
        cleaned.etp_total

    from cleaned

    left join {{ ref('seed_fagerh__metier_family_mapping') }} as metier_mapping
        on cleaned.metier = metier_mapping.metier

)

select *
from final
