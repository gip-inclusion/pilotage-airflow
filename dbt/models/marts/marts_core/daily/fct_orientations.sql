with orientations as (
    select * from {{ ref('stg_dora__orientation') }}
),

emplois_orientation_data as (
    select * from {{ ref('stg_dora__emplois_orientation_data') }}
),

structure_source_mapping as (
    select * from {{ ref('int_di__structure_source_mapping') }}
),

data_inclusion_services as (
    select service_id from {{ ref('dim_data_inclusion__services') }}
),

final as (
    select
        emplois_orientation_data.emplois_sync_uid,
        orientations.creation_date,
        orientations.processing_date,
        orientations.status,
        orientations.prescriber_id,
        emplois_orientation_data.beneficiary_id as emplois_beneficiary_id,
        emplois_orientation_data.prescriber_id  as emplois_prescriber_id,
        cast(orientations.id as text)           as id,
        case
            when orientations.prescriber_id is null then emplois_prescriber_structure_mapping.structure_id
            else dora_prescriber_structure_mapping.structure_id
        end                                     as prescriber_di_structure_id,
        case
            when orientations.prescriber_id is not null then orientations.prescriber_structure_id
        end                                     as prescriber_dora_structure_id,
        case
            when orientations.prescriber_id is null then emplois_orientation_data.structure_id
        end                                     as prescriber_emplois_structure_id,
        case
            when orientations.service_id is not null then dora_oriented_service.service_id
            else nullif(orientations.di_service_id, '')
        end                                     as oriented_di_service_id,
        orientations.service_id                 as oriented_dora_service_id,
        case
            when orientations.prescriber_id is null then 'emplois'
            else 'dora'
        end                                     as origin_source
    from orientations
    left join emplois_orientation_data
        on orientations.id = emplois_orientation_data.orientation_id
    left join structure_source_mapping as dora_prescriber_structure_mapping
        on
            dora_prescriber_structure_mapping.source_structure_id
            = 'dora--' || cast (orientations.prescriber_structure_id as text)
    left join structure_source_mapping as emplois_prescriber_structure_mapping
        on
            emplois_prescriber_structure_mapping.source_structure_id
            = 'emplois-de-linclusion--' || cast (emplois_orientation_data.structure_id as text)
    left join data_inclusion_services as dora_oriented_service
        on dora_oriented_service.service_id = 'dora--' || cast (orientations.service_id as text)
    where
        orientations.prescriber_id is not null
        or emplois_orientation_data.orientation_id is not null
)

select
    id,
    origin_source,
    emplois_sync_uid,
    creation_date,
    processing_date,
    status,
    prescriber_id,
    prescriber_di_structure_id,
    prescriber_dora_structure_id,
    prescriber_emplois_structure_id,
    oriented_di_service_id,
    oriented_dora_service_id,
    emplois_beneficiary_id,
    emplois_prescriber_id
from final
