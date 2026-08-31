with emplois_imer as (
    select * from {{ ref('stg_emplois__imer') }}
),

dora_imer as (
    select * from {{ ref('int_dora__imer') }}
),

dora_orientations as (
    select * from {{ ref('fct_dora__orientations') }}
),

data_inclusion_services as (
    select
        service_id,
        source
    from {{ ref('dim_data_inclusion__services') }}
),

structure_source_mapping as (
    select * from {{ ref('int_di__structure_source_mapping') }}
),

emplois as (
    select
        'emplois'                               as origin_source,
        cast(emplois_imer.id as text)           as source_imer_id,
        emplois_imer.date,
        emplois_imer.user_session,
        emplois_imer.user_kind,
        cast(emplois_imer.user_id as text)      as user_id,
        emplois_imer.user_prescriber_organization_id,
        emplois_imer.user_company_id,
        cast(emplois_imer.structure_id as text) as target_structure_source_id,
        structure_source_mapping.structure_id   as target_di_structure_id,
        emplois_imer.kind,
        cast(emplois_imer.service_id as text)   as service_id,
        emplois_imer.source,
        emplois_imer.external_link,
        dora_orientations.id                    as orientation_id,
        emplois_imer.date_mise_a_jour_metabase
    from emplois_imer
    left join structure_source_mapping
        on cast(emplois_imer.structure_id as text) = structure_source_mapping.source_structure_id
    left join dora_orientations
        on cast(emplois_imer.orientation_id as text) = cast(dora_orientations.emplois_sync_uid as text)
),

dora_imer_with_orientation as materialized (
    select
        dora_imer.origin_source,
        dora_imer.event_id,
        dora_imer.date,
        dora_imer.user_kind,
        dora_imer.user_id,
        dora_imer.target_structure_source_id,
        dora_imer.target_di_structure_id,
        dora_imer.kind,
        dora_imer.mobilized_service_id,
        dora_imer.is_di_service,
        dora_orientations.oriented_service_id_di,
        dora_orientations.id as orientation_id
    from dora_imer
    left join dora_orientations
        on dora_imer.orientation_id = cast(dora_orientations.id as text)
),

dora_imer_with_confirmed_service as (
    select
        dora_imer_with_orientation.*,
        coalesce(
            orientation_service.service_id,
            mobilized_service.service_id
        ) as service_id,
        coalesce(
            orientation_service.source,
            mobilized_service.source,
            case when dora_imer_with_orientation.is_di_service then 'data_inclusion' else 'dora' end
        ) as source
    from dora_imer_with_orientation
    left join data_inclusion_services as orientation_service
        on dora_imer_with_orientation.oriented_service_id_di = orientation_service.service_id
    left join data_inclusion_services as mobilized_service
        on dora_imer_with_orientation.mobilized_service_id = mobilized_service.service_id
),

dora as (
    select
        origin_source,
        event_id                as source_imer_id,
        date,
        cast(null as text)      as user_session,
        user_kind,
        cast(user_id as text)   as user_id,
        cast(null as integer)   as user_prescriber_organization_id,
        cast(null as integer)   as user_company_id,
        target_structure_source_id,
        target_di_structure_id,
        kind,
        service_id,
        source,
        cast(null as text)      as external_link,
        orientation_id,
        cast(null as timestamp) as date_mise_a_jour_metabase
    from dora_imer_with_confirmed_service
),

all_imer as (
    select * from emplois
    union all
    select * from dora
)

select
    {{ dbt_utils.generate_surrogate_key([
        'origin_source',
        'kind',
        'source_imer_id'
    ]) }} as id,
    origin_source,
    source_imer_id,
    date,
    user_session,
    user_kind,
    user_id,
    user_prescriber_organization_id,
    user_company_id,
    target_structure_source_id,
    target_di_structure_id,
    kind,
    service_id,
    source,
    external_link,
    orientation_id,
    date_mise_a_jour_metabase
from all_imer
