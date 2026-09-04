with orientations as (
    select * from {{ ref('stg_dora__orientation') }}
),

emplois_orientation_data as (
    select * from {{ ref('stg_dora__emplois_orientation_data') }}
),

structure_source_mapping as (
    select * from {{ ref('int_di__structure_source_mapping') }}
),

dora_structures as (
    select
        id,
        name,
        siret,
        structure_id_jointure_di
    from {{ ref('stg_dora__structure') }}
),

data_inclusion_services as (
    select
        service_id,
        code_commune_insee
    from {{ ref('dim_data_inclusion__services') }}
),

dora_services as (
    select * from {{ ref('dim_dora__services') }}
),

orientation_sources as (
    select
        emplois_orientation_data.emplois_sync_uid,
        orientations.creation_date,
        orientations.processing_date,
        orientations.status,
        orientations.prescriber_id,
        emplois_orientation_data.beneficiary_id  as emplois_beneficiary_id,
        emplois_orientation_data.prescriber_id   as emplois_prescriber_id,
        cast(orientations.id as text)            as id,
        orientations.prescriber_structure_id,
        emplois_orientation_data.structure_id    as emplois_structure_id,
        emplois_orientation_data.structure_name  as emplois_structure_name,
        emplois_orientation_data.structure_siret as emplois_structure_siret,
        orientations.service_id                  as dora_service_id,
        orientations.di_service_name,
        orientations.di_structure_name,
        orientations.original_service_name,
        nullif(orientations.di_service_id, '')   as direct_di_service_id,
        case
            when orientations.prescriber_id is null then 'emplois'
            else 'dora'
        end                                      as origin_source
    from orientations
    left join emplois_orientation_data
        on orientations.id = emplois_orientation_data.orientation_id
    where
        orientations.prescriber_id is not null
        or emplois_orientation_data.orientation_id is not null
),

orientations_enriched as (
    select
        orientation_sources.id,
        orientation_sources.origin_source,
        orientation_sources.emplois_sync_uid,
        orientation_sources.creation_date,
        orientation_sources.processing_date,
        orientation_sources.status,
        orientation_sources.prescriber_id         as prescriber_id_dora,
        orientation_sources.dora_service_id       as oriented_service_id_dora,
        orientation_sources.emplois_beneficiary_id,
        orientation_sources.emplois_prescriber_id as prescriber_id_emplois,
        case
            when orientation_sources.origin_source = 'emplois' then emplois_prescriber_structure_mapping.structure_id
            else dora_prescriber_structure_mapping.structure_id
        end                                       as prescriber_structure_id_di,
        case
            when orientation_sources.origin_source = 'dora' then orientation_sources.prescriber_structure_id
        end                                       as prescriber_structure_id_dora,
        case
            when orientation_sources.origin_source = 'emplois' then orientation_sources.emplois_structure_id
        end                                       as prescriber_structure_id_emplois,
        case
            when orientation_sources.origin_source = 'emplois' then orientation_sources.emplois_structure_name
            else dora_structures.name
        end                                       as prescriber_structure_name,
        case
            when orientation_sources.origin_source = 'emplois' then orientation_sources.emplois_structure_siret
            else dora_structures.siret
        end                                       as prescriber_structure_siret,
        case
            when orientation_sources.dora_service_id is not null then data_inclusion_oriented_service.service_id
            else orientation_sources.direct_di_service_id
        end                                       as oriented_service_id_di,
        coalesce(
            orientation_sources.di_service_name,
            dora_oriented_service.name,
            orientation_sources.original_service_name
        )                                         as oriented_service_name,
        coalesce(
            orientation_sources.di_structure_name,
            dora_oriented_service.structure_name
        )                                         as oriented_service_structure_name,
        coalesce(
            data_inclusion_oriented_service.code_commune_insee,
            dora_oriented_service.city_code
        )                                         as oriented_service_code_commune_insee
    from orientation_sources
    left join dora_structures
        on orientation_sources.prescriber_structure_id = dora_structures.id
    left join structure_source_mapping as dora_prescriber_structure_mapping
        on dora_structures.structure_id_jointure_di = dora_prescriber_structure_mapping.source_structure_id
    left join structure_source_mapping as emplois_prescriber_structure_mapping
        on
            emplois_prescriber_structure_mapping.source_structure_id
            = 'emplois-de-linclusion--' || cast(orientation_sources.emplois_structure_id as text)
    left join dora_services as dora_oriented_service
        on orientation_sources.dora_service_id = dora_oriented_service.id
    left join data_inclusion_services as data_inclusion_oriented_service
        on
            data_inclusion_oriented_service.service_id
            = case
                when orientation_sources.dora_service_id is not null then dora_oriented_service.service_id_jointure_di
                else orientation_sources.direct_di_service_id
            end
),

final as (
    select
        id,
        origin_source,
        emplois_sync_uid,
        creation_date,
        processing_date,
        status,
        prescriber_id_dora,
        prescriber_id_emplois,
        prescriber_structure_id_di,
        prescriber_structure_id_dora,
        prescriber_structure_id_emplois,
        prescriber_structure_name,
        prescriber_structure_siret,
        oriented_service_id_di,
        oriented_service_id_dora,
        oriented_service_name,
        oriented_service_structure_name,
        oriented_service_code_commune_insee,
        emplois_beneficiary_id
    from orientations_enriched
)

select *
from final
