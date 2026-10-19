with orientations as (
    select * from {{ ref('stg_dora__orientation') }}
    where prescriber_id is not null
),

emplois_orientation_data as (
    select * from {{ ref('stg_dora__emplois_orientation_data') }}
),

dora_users as (
    -- La dimension dim_dora__users ne garde que les utilisateurs actifs, valides et non staff.
    -- Le staging permet de retrouver le type des auteurs d'orientations historiques même
    -- s'ils ne remplissent plus ces critères, tant que leur profil existe dans la source.
    select
        id,
        main_activity
    from {{ ref('stg_dora__user') }}
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
)

select
    orientations.id::text                     as id,
    'dora'                                    as origin_source,
    orientations.creation_date,
    orientations.processing_date,
    orientations.status,
    orientations.requirements,
    orientations.situation,
    orientations.beneficiary_contact_preferences,
    orientations.beneficiary_availability,
    orientations.duration_weekly_hours,
    orientations.duration_weeks,
    orientations.query_expires_at,
    orientations.last_reminder_email_sent,
    orientations.data_protection_commitment,
    orientations.is_anonymized,
    orientations.prescriber_id                as prescriber_id_dora,
    emplois_orientation_data.prescriber_id    as prescriber_id_emplois,
    prescriber_structure_mapping.structure_id as prescriber_structure_id_di,
    orientations.prescriber_structure_id      as prescriber_structure_id_dora,
    dora_structures.name                      as prescriber_structure_name,
    dora_structures.siret                     as prescriber_structure_siret,
    orientations.service_id                   as oriented_service_id_dora,
    -- UUID public reçu via le JWT : la jointure candidat se fait sur candidats_v0.uid, pas sur son id.
    -- TODO : passer à l'id du candidat via dim_emplois__candidats quand la dimension sera disponible
    emplois_orientation_data.beneficiary_id   as emplois_beneficiary_id,
    nullif(dora_users.main_activity, '')      as user_kind,
    case
        when orientations.service_id is not null then data_inclusion_oriented_service.service_id
        else nullif(orientations.di_service_id, '')
    end                                       as oriented_service_id_di,
    coalesce(
        orientations.di_service_name,
        dora_oriented_service.name,
        orientations.original_service_name
    )                                         as oriented_service_name,
    coalesce(
        orientations.di_structure_name,
        dora_oriented_service.structure_name
    )                                         as oriented_service_structure_name,
    coalesce(
        data_inclusion_oriented_service.code_commune_insee,
        dora_oriented_service.city_code
    )                                         as oriented_service_code_commune_insee
from orientations
left join emplois_orientation_data
    on orientations.id = emplois_orientation_data.orientation_id
left join dora_users
    on orientations.prescriber_id = dora_users.id
left join dora_structures
    on orientations.prescriber_structure_id = dora_structures.id
left join structure_source_mapping as prescriber_structure_mapping
    on dora_structures.structure_id_jointure_di = prescriber_structure_mapping.source_structure_id
left join dora_services as dora_oriented_service
    on orientations.service_id = dora_oriented_service.id
left join data_inclusion_services as data_inclusion_oriented_service
    on
        data_inclusion_oriented_service.service_id
        = case
            when orientations.service_id is not null then dora_oriented_service.service_id_jointure_di
            else nullif(orientations.di_service_id, '')
        end
