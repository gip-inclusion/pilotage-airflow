with mobilisation_events as (
    select
        *,
        nullif(split_part(event_path, '/', 3), '')                      as mobilized_service_slug,
        nullif(replace(split_part(event_path, '/', 3), 'di--', ''), '') as mobilized_data_inclusion_service_id
    from {{ ref('int_dora__mobilisationevent_user') }}
),

mobilisation_to_orientation as (
    select * from {{ ref('int_dora__mobilisation_to_orientation') }}
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
        id_jointure_di
    from {{ ref('dim_dora__services') }}
    where slug is not null
    order by
        slug,
        id
),

dora_mobilized_services as (
    select
        mobilisation_events.event_id,
        data_inclusion_services.service_id as mobilized_service_id
    from mobilisation_events
    inner join dora_services
        on mobilisation_events.mobilized_service_slug = dora_services.slug
    inner join data_inclusion_services
        on dora_services.id_jointure_di = data_inclusion_services.service_id
    where mobilisation_events.event_structure_source = 'dora'
),

data_inclusion_mobilized_services as (
    select
        mobilisation_events.event_id,
        data_inclusion_services.service_id as mobilized_service_id
    from mobilisation_events
    inner join data_inclusion_services
        on mobilisation_events.mobilized_data_inclusion_service_id = data_inclusion_services.service_id
    where mobilisation_events.event_structure_source = 'data_inclusion'
),

confirmed_mobilized_services as (
    select * from dora_mobilized_services
    union all
    select * from data_inclusion_mobilized_services
),

imer as (
    select
        mobilisation_events.event_id,
        mobilisation_events.event_date                                           as date, -- noqa: RF04
        mobilisation_events.user_id,
        mobilisation_events.event_user_kind                                      as user_kind,
        mobilisation_events.event_is_manager                                     as is_manager,
        mobilisation_events.event_structure_id_di_source                         as target_structure_source_id,
        structure_source_mapping.structure_id                                    as target_di_structure_id,
        mobilisation_events.event_is_di                                          as is_di_service,
        confirmed_mobilized_services.mobilized_service_id,
        mobilisation_to_orientation.generates_orientation,
        'mobilisation'                                                           as kind,
        cast(mobilisation_to_orientation.first_following_orientation_id as text) as orientation_id,
        case
            when mobilisation_events.event_structure_source = 'data_inclusion' then 'dora-data-inclusion'
            else 'dora'
        end                                                                      as origin_source,
        coalesce(
            mobilisation_events.user_main_activity in ('accompagnateur', 'accompagnateur_offreur'),
            false
        )                                                                        as is_prescriber
    from mobilisation_events
    left join mobilisation_to_orientation
        on mobilisation_events.event_id = mobilisation_to_orientation.mobilisation_id
    left join structure_source_mapping
        on mobilisation_events.event_structure_id_di_source = structure_source_mapping.source_structure_id
    left join confirmed_mobilized_services
        on mobilisation_events.event_id = confirmed_mobilized_services.event_id
),

final as (
    select
        kind,
        event_id,
        orientation_id,
        date,
        user_id,
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
