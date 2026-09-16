with mobilisation_events as (
    select
        *,
        nullif(split_part(path, '/', 3), '')                      as mobilized_service_slug,
        nullif(replace(split_part(path, '/', 3), 'di--', ''), '') as mobilized_data_inclusion_service_id
    from {{ ref('stg_dora__mobilisationevent') }}
),

structure_info_views as (
    select *
    from {{ ref('stg_dora__structureinfosview') }}
    where
        is_staff is false
        and is_structure_member is false
        and is_structure_admin is false
),

mobilisation_to_orientation as (
    select * from {{ ref('int_dora__mobilisation_to_orientation') }}
),

active_users as (
    select * from {{ ref('int_dora__active_user') }}
),

structure_source_mapping as (
    select * from {{ ref('int_di__structure_source_mapping') }}
),

data_inclusion_services as (
    select service_id
    from {{ ref('dim_data_inclusion__services') }}
),

dora_services as (
    select distinct on (slug)
        slug,
        service_id_jointure_di
    from {{ ref('dim_dora__services') }}
    where slug is not null
    order by
        slug,
        id
),

dora_mobilized_services as (
    select
        mobilisation_events.id,
        data_inclusion_services.service_id as mobilized_service_id
    from mobilisation_events
    inner join dora_services
        on mobilisation_events.mobilized_service_slug = dora_services.slug
    inner join data_inclusion_services
        on dora_services.service_id_jointure_di = data_inclusion_services.service_id
    where mobilisation_events.structure_source = 'dora'
),

data_inclusion_mobilized_services as (
    select
        mobilisation_events.id,
        data_inclusion_services.service_id as mobilized_service_id
    from mobilisation_events
    inner join data_inclusion_services
        on mobilisation_events.mobilized_data_inclusion_service_id = data_inclusion_services.service_id
    where mobilisation_events.structure_source = 'data_inclusion'
),

confirmed_mobilized_services as (
    select * from dora_mobilized_services
    union all
    select * from data_inclusion_mobilized_services
),

mobilisation_imer as (
    select
        mobilisation_events.id                                   as event_id,
        mobilisation_events.date, -- noqa: RF04
        mobilisation_events.user_id,
        mobilisation_events.is_logged,
        mobilisation_events.user_kind,
        mobilisation_events.is_manager,
        mobilisation_events.structure_id_di_source               as target_structure_source_id,
        structure_source_mapping.structure_id                    as target_di_structure_id,
        mobilisation_events.is_di                                as is_di_service,
        confirmed_mobilized_services.mobilized_service_id,
        'mobilisation'                                           as kind,
        cast(mobilisation_to_orientation.orientation_id as text) as orientation_id,
        mobilisation_to_orientation.orientation_id is not null   as generates_orientation,
        case
            when mobilisation_events.structure_source = 'data_inclusion' then 'dora-data-inclusion'
            else 'dora'
        end                                                      as origin_source,
        coalesce(
            active_users.main_activity in ('accompagnateur', 'accompagnateur_offreur'),
            false
        )                                                        as is_prescriber
    from mobilisation_events
    left join mobilisation_to_orientation
        on mobilisation_events.id = mobilisation_to_orientation.mobilisation_id
    left join active_users
        on mobilisation_events.user_id = active_users.id
    left join structure_source_mapping
        on mobilisation_events.structure_id_di_source = structure_source_mapping.source_structure_id
    left join confirmed_mobilized_services
        on mobilisation_events.id = confirmed_mobilized_services.id
),

structure_contact_imer as (
    select
        structure_info_views.id                     as event_id,
        structure_info_views.date, -- noqa: RF04
        structure_info_views.user_id,
        structure_info_views.is_logged,
        structure_info_views.user_kind,
        structure_info_views.is_manager,
        structure_info_views.structure_id_di_source as target_structure_source_id,
        structure_source_mapping.structure_id       as target_di_structure_id,
        false                                       as is_di_service,
        cast(null as text)                          as mobilized_service_id,
        'structure_contact'                         as kind,
        cast(null as text)                          as orientation_id,
        false                                       as generates_orientation,
        'dora'                                      as origin_source,
        coalesce(
            active_users.main_activity in ('accompagnateur', 'accompagnateur_offreur'),
            false
        )                                           as is_prescriber
    from structure_info_views
    left join active_users
        on structure_info_views.user_id = active_users.id
    left join structure_source_mapping
        on structure_info_views.structure_id_di_source = structure_source_mapping.source_structure_id
),

imer as (
    select * from mobilisation_imer
    union all
    select * from structure_contact_imer
),

final as (
    select
        kind,
        event_id,
        orientation_id,
        date,
        user_id,
        is_logged,
        user_kind,
        is_manager,
        origin_source,
        target_structure_source_id,
        target_di_structure_id,
        mobilized_service_id,
        is_di_service,
        is_prescriber,
        generates_orientation
    from imer
)

select *
from final
