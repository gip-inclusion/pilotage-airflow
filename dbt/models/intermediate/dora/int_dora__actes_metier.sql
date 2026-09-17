with user_struct as (

    select distinct on (dora_users.id)
        dora_users.id as user_id,
        structures.typology
    from {{ source('dora', 'users_user') }} as dora_users
    inner join {{ source('dora', 'structures_structuremember') }} as structure_members
        on dora_users.id = structure_members.user_id
    inner join {{ source('dora', 'structures_structure') }} as structures
        on structure_members.structure_id = structures.id
    where structures.typology is not null and structures.typology != ''
    order by dora_users.id

),

orientations as (

    select
        orientations.creation_date,
        orientations.status,
        oriented_service_communes.code_departement_insee as raw_departement,
        case
            when orientations.origin_source = 'dora' then prescriber_structures.typology
        end                                              as raw_type_structure
    from {{ ref('fct_dora__orientations') }} as orientations
    left join {{ ref('dim_dora__structures') }} as prescriber_structures
        on orientations.prescriber_structure_id_dora = prescriber_structures.id
    left join {{ ref('dim_commune') }} as oriented_service_communes
        on orientations.oriented_service_code_commune_insee = oriented_service_communes.code_commune_insee

),

imer as (

    select
        imer.date,
        case
            when
                imer.origin_source = 'emplois'
                and imer.user_prescriber_organization_id is not null
                then 'emplois_organisation'
            when
                imer.origin_source = 'emplois'
                and imer.user_company_id is not null
                then 'emplois_structure'
            when imer.origin_source in ('dora', 'dora-data-inclusion') then user_struct.typology
        end as raw_type_structure,
        coalesce(
            target_structure_communes.code_departement_insee,
            case
                when imer.origin_source in ('dora', 'dora-data-inclusion')
                    then mobilisation_events.structure_department
            end
        )   as raw_departement
    from {{ ref('fct_dora__imer') }} as imer
    left join user_struct
        on
            imer.origin_source in ('dora', 'dora-data-inclusion')
            and cast(user_struct.user_id as text) = cast(imer.user_id as text)
    left join {{ ref('dim_data_inclusion__structures') }} as target_structures
        on imer.target_di_structure_id = target_structures.structure_id
    left join {{ ref('dim_commune') }} as target_structure_communes
        on target_structures.code_commune_insee = target_structure_communes.code_commune_insee
    left join {{ ref('stg_dora__mobilisationevent') }} as mobilisation_events
        on
            imer.origin_source in ('dora', 'dora-data-inclusion')
            and imer.source_imer_id = mobilisation_events.id
    where imer.user_kind in (
        'accompagnateur',
        'accompagnateur_offreur',
        'offreur',
        'emplois_employer',
        'emplois_prescriber'
    )

),

raw_actes as (

    select
        to_char(date_trunc('month', orientations.creation_date), 'YYYY-MM') as mois,
        'dora'                                                              as source,
        'Orientation vers service'                                          as type_acte,
        'Accompagnement'                                                    as categorie_acte,
        orientations.status in ('VALIDÉE', 'REFUSÉE')                       as north_star,
        orientations.status in ('VALIDÉE', 'REFUSÉE')                       as north_star_70,
        orientations.status in ('VALIDÉE', 'REFUSÉE')                       as traite,
        orientations.raw_type_structure,
        coalesce(orientations.raw_departement, 'Inconnu')                   as raw_departement,
        count(*)                                                            as nombre_actes
    from orientations
    where
        orientations.creation_date >= date_trunc('month', current_date) - interval '14 months'
        and orientations.creation_date < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', orientations.creation_date), 'YYYY-MM'),
        orientations.status in ('VALIDÉE', 'REFUSÉE'),
        orientations.raw_type_structure,
        coalesce(orientations.raw_departement, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', imer.date), 'YYYY-MM') as mois,
        'dora'                                             as source,
        'Intention d’orientation'                          as type_acte,
        'Support'                                          as categorie_acte,
        false                                              as north_star,
        false                                              as north_star_70,
        false                                              as traite,
        imer.raw_type_structure,
        coalesce(imer.raw_departement, 'Inconnu')          as raw_departement,
        count(*)                                           as nombre_actes
    from imer
    where
        imer.date >= date_trunc('month', current_date) - interval '14 months'
        and imer.date < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', imer.date), 'YYYY-MM'),
        imer.raw_type_structure,
        coalesce(imer.raw_departement, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', services.modification_date), 'YYYY-MM') as mois,
        'dora'                                                              as source,
        'Mise à jour offre de service, hors emploi solidaire'               as type_acte,
        'Support'                                                           as categorie_acte,
        false                                                               as north_star,
        false                                                               as north_star_70,
        false                                                               as traite,
        structures.typology                                                 as raw_type_structure,
        coalesce(structures.department, 'Inconnu')                          as raw_departement,
        count(*)                                                            as nombre_actes
    from {{ source('dora', 'services_service') }} as services
    left join {{ source('dora', 'structures_structure') }} as structures
        on services.structure_id = structures.id
    where
        services.last_editor_id is not null
        and services.modification_date >= date_trunc('month', current_date) - interval '14 months'
        and services.modification_date < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', services.modification_date), 'YYYY-MM'),
        structures.typology,
        coalesce(structures.department, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', services.creation_date), 'YYYY-MM') as mois,
        'dora'                                                          as source,
        'Création ou diffusion offre de service, hors emploi solidaire' as type_acte,
        'Support'                                                       as categorie_acte,
        false                                                           as north_star,
        false                                                           as north_star_70,
        false                                                           as traite,
        structures.typology                                             as raw_type_structure,
        coalesce(structures.department, 'Inconnu')                      as raw_departement,
        count(*)                                                        as nombre_actes
    from {{ source('dora', 'services_service') }} as services
    left join {{ source('dora', 'structures_structure') }} as structures
        on services.structure_id = structures.id
    where
        services.creation_date >= date_trunc('month', current_date) - interval '14 months'
        and services.creation_date < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', services.creation_date), 'YYYY-MM'),
        structures.typology,
        coalesce(structures.department, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', structures.creation_date), 'YYYY-MM') as mois,
        'dora'                                                            as source,
        'Création structure, hors employeur solidaire'                    as type_acte,
        'Support'                                                         as categorie_acte,
        false                                                             as north_star,
        false                                                             as north_star_70,
        false                                                             as traite,
        structures.typology                                               as raw_type_structure,
        coalesce(structures.department, 'Inconnu')                        as raw_departement,
        count(*)                                                          as nombre_actes
    from {{ source('dora', 'structures_structure') }} as structures
    where
        structures.creation_date >= date_trunc('month', current_date) - interval '14 months'
        and structures.creation_date < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', structures.creation_date), 'YYYY-MM'),
        structures.typology,
        coalesce(structures.department, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', structures.modification_date), 'YYYY-MM')      as mois,
        'dora'                                                                     as source,
        'Mise à jour d’une structure d’offre de service, hors employeur solidaire' as type_acte,
        'Support'                                                                  as categorie_acte,
        false                                                                      as north_star,
        false                                                                      as north_star_70,
        false                                                                      as traite,
        structures.typology                                                        as raw_type_structure,
        coalesce(structures.department, 'Inconnu')                                 as raw_departement,
        count(*)                                                                   as nombre_actes
    from {{ source('dora', 'structures_structure') }} as structures
    where
        structures.modification_date is not null
        and structures.modification_date > structures.creation_date
        and structures.modification_date >= date_trunc('month', current_date) - interval '14 months'
        and structures.modification_date < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', structures.modification_date), 'YYYY-MM'),
        structures.typology,
        coalesce(structures.department, 'Inconnu')

    union all

    select
        to_char(date_trunc('month', search_views.date), 'YYYY-MM') as mois,
        'dora'                                                     as source,
        'Recherche d’offre de service, hors emploi solidaire'      as type_acte,
        'Support'                                                  as categorie_acte,
        false                                                      as north_star,
        false                                                      as north_star_70,
        false                                                      as traite,
        user_struct.typology                                       as raw_type_structure,
        coalesce(search_views.department, 'Inconnu')               as raw_departement,
        count(*)                                                   as nombre_actes
    from {{ source('dora', 'stats_searchview') }} as search_views
    left join user_struct
        on cast(user_struct.user_id as text) = cast(search_views.user_id as text)
    where
        search_views.date >= date_trunc('month', current_date) - interval '14 months'
        and search_views.date < date_trunc('month', current_date)
    group by
        to_char(date_trunc('month', search_views.date), 'YYYY-MM'),
        user_struct.typology,
        coalesce(search_views.department, 'Inconnu')

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
        nombre_actes,
        case
            when raw_type_structure is null or trim(cast(raw_type_structure as text)) = '' then 'Autre'
            when raw_type_structure in ('FT', 'PE', 'france_travail') then 'FRANCE_TRAVAIL'
            when raw_type_structure in ('ML', 'mission_locale') then 'MISSION_LOCALE'
            when raw_type_structure in ('CAP_EMPLOI', 'cap_emploi') then 'CAP_EMPLOI'
            when raw_type_structure in ('DEPT', 'CD', 'conseil_departemental') then 'CONSEIL_DEPARTEMENTAL'
            when raw_type_structure in ('ODC', 'delegataire_rsa', 'DELEGATAIRE_RSA') then 'DELEGATAIRE_RSA'
            when raw_type_structure in ('CCAS', 'CIAS', 'ASE') then 'CCAS_CIAS'
            when raw_type_structure in ('SPIP', 'PJJ', 'JUSTICE') then 'JUSTICE_PROBATION'
            when raw_type_structure in (
                'CHU', 'CHRS', 'CPH', 'CADA', 'HUDA', 'RS_FJT', 'OIL', 'PENSION', 'ACT', 'LHSS',
                'CHRS/Accueil de jour', 'Accueil de jour'
            ) then 'TS_HEBERGEMENT'
            when raw_type_structure in (
                'EI', 'AI', 'ACI', 'ETTI', 'EITI', 'GEIQ', 'EA', 'EATT', 'OPCS', 'siae', 'employer',
                'Groupements d’employeurs pour l’insertion et la qualification', 'SIAE'
            ) then 'SIAE'
            when raw_type_structure = 'PLIE' then 'PLIE'
            when raw_type_structure in ('E2C', 'EPIDE', 'AFPA', 'OF', 'Organisme de formation') then 'E2C_EPIDE_AFPA'
            when raw_type_structure in ('CIDFF', 'CSAPA', 'CAARUD', 'PREVENTION', 'OHPD', 'OCASF') then 'TS_SPECIALISES'
            when raw_type_structure in ('CAF', 'MSA') then 'CAF_MSA'
            when raw_type_structure in ('PIJ_BIJ', 'OACAS', 'CAVA') then 'AUTRES_INSERTION'
            when raw_type_structure in ('emplois_organisation', 'emplois_structure') then raw_type_structure
            when raw_type_structure in ('BUYER', 'PARTNER', 'INDIVIDUAL', 'ADMIN') then 'Autre'
            else 'Autre'
        end as type_structure,
        case
            when raw_departement is null or trim(cast(raw_departement as text)) = '' then 'Inconnu'
            when upper(split_part(trim(cast(raw_departement as text)), ' - ', 1)) in ('2A', '2B')
                then upper(split_part(trim(cast(raw_departement as text)), ' - ', 1))
            when
                split_part(trim(cast(raw_departement as text)), ' - ', 1) ~ '^[0-9]+$'
                and cast(split_part(trim(cast(raw_departement as text)), ' - ', 1) as integer) between 1 and 95
                then lpad(split_part(trim(cast(raw_departement as text)), ' - ', 1), 2, '0')
            when
                split_part(trim(cast(raw_departement as text)), ' - ', 1) ~ '^[0-9]+$'
                and cast(split_part(trim(cast(raw_departement as text)), ' - ', 1) as integer) between 971 and 976
                then split_part(trim(cast(raw_departement as text)), ' - ', 1)
            else 'Inconnu'
        end as departement
    from raw_actes

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
