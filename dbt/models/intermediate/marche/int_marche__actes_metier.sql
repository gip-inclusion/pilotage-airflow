with raw_actes as (

    select
        to_char(date_trunc('month', tenders.created_at), 'YYYY-MM') as mois,
        'marche'                                                    as source,
        'Diffusion d’offre inclusive'                               as type_acte,
        'Support'                                                   as categorie_acte,
        coalesce(tenders.siae_count > 0, false)                     as north_star,
        coalesce(tenders.siae_count > 0, false)                     as north_star_70,
        coalesce(tenders.siae_count > 0, false)                     as traite,
        case marche_users.kind
            when 'SIAE' then 'SIAE'
            else 'Autre'
        end                                                         as type_structure,
        coalesce(perimeters.department_code, 'Inconnu')             as raw_departement,
        count(*)                                                    as nombre_actes
    from {{ ref('stg_marche__tenders') }} as tenders
    left join {{ ref('stg_marche__users') }} as marche_users
        on tenders.author_id = marche_users.id
    left join {{ ref('stg_marche__perimeters') }} as perimeters
        on tenders.location_id = perimeters.id
    where
        tenders.kind in ('TENDER', 'PROJ', 'QUOTE')
        and tenders.status in ('SUBMITTED', 'SENT')
        and tenders.created_at >= date_trunc('month', current_date) - interval '14 months'
        and tenders.created_at < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', tenders.created_at), 'YYYY-MM'),
        coalesce(tenders.siae_count > 0, false),
        case marche_users.kind
            when 'SIAE' then 'SIAE'
            else 'Autre'
        end,
        coalesce(perimeters.department_code, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', siae_offers.updated_at), 'YYYY-MM') as mois,
        'marche'                                                        as source,
        'Mise à jour fiche entreprise'                                  as type_acte,
        'Support'                                                       as categorie_acte,
        false                                                           as north_star,
        false                                                           as north_star_70,
        false                                                           as traite,
        'SIAE'                                                          as type_structure,
        coalesce(siaes.department, 'Inconnu')                           as raw_departement,
        count(*)                                                        as nombre_actes
    from {{ ref('stg_marche__siae_offers') }} as siae_offers
    inner join {{ ref('stg_marche__siaes') }} as siaes
        on siae_offers.siae_id = siaes.id
    where
        siae_offers.updated_at >= date_trunc('month', current_date) - interval '14 months'
        and siae_offers.updated_at < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', siae_offers.updated_at), 'YYYY-MM'),
        coalesce(siaes.department, 'Inconnu')

),

department_tokens as (

    select
        mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        raw_departement,
        nombre_actes,
        split_part(trim(cast(raw_departement as text)), ' - ', 1) as departement_token
    from raw_actes

),

normalized as (

    select
        mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        nombre_actes,
        case
            when raw_departement is null or trim(cast(raw_departement as text)) = '' then 'Inconnu'
            when upper(departement_token) in ('2A', '2B')
                then upper(departement_token)
            when
                departement_token ~ '^[0-9]+$'
                and cast(departement_token as integer) between 1 and 95
                then lpad(cast(cast(departement_token as integer) as text), 2, '0')
            when
                departement_token ~ '^[0-9]+$'
                and cast(departement_token as integer) between 971 and 976
                then cast(cast(departement_token as integer) as text)
            else 'Inconnu'
        end as departement
    from department_tokens

)

select
    mois,
    source,
    type_acte,
    categorie_acte,
    north_star,
    north_star_70,
    traite,
    type_structure,
    departement,
    cast(sum(nombre_actes) as integer) as nombre_actes
from normalized
where nombre_actes > 0
group by
    mois,
    source,
    type_acte,
    categorie_acte,
    north_star,
    north_star_70,
    traite,
    type_structure,
    departement
