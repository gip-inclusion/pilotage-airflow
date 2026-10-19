with orientations as (
    select * from {{ ref('stg_emplois__orientations') }}
),

services as (
    select * from {{ ref('stg_emplois__services') }}
),

organisations as (
    select * from {{ ref('stg_organisations') }}
),

companies as (
    select * from {{ ref('stg_structures') }}
),

structure_source_mapping as (
    select * from {{ ref('int_di__structure_source_mapping') }}
),

data_inclusion_structures as (
    select * from {{ ref('dim_data_inclusion__structures') }}
),

senders as (
    select
        orientations.*,
        case
            when orientations.sender_kind = 'emplois_prescriber' then orientations.sender_prescriber_organization_uid
            when orientations.sender_kind = 'emplois_employer' then orientations.sender_company_uid
        end as sender_structure_uid
    from orientations
)

select
    senders.id::text                           as id,
    'emplois'                                  as origin_source,
    senders.creation_date,
    senders.processing_date,
    senders.status,
    services.duration_weekly_hours,
    services.duration_weeks,
    senders.last_reminder_email_sent_at        as last_reminder_email_sent,
    senders.data_protection_commitment,
    senders.sender_public_id                   as prescriber_id_emplois,
    senders.sender_kind                        as user_kind,
    prescriber_structure_mapping.structure_id  as prescriber_structure_id_di,
    senders.sender_company_uid                 as prescriber_structure_id_emplois,
    senders.sender_prescriber_organization_uid as prescriber_organization_id_emplois,
    senders.service_uid                        as oriented_service_id_di,
    services.name                              as oriented_service_name,
    data_inclusion_structures.nom              as oriented_service_structure_name,
    services.code_insee                        as oriented_service_code_commune_insee,
    -- UUID public : la jointure candidat se fait sur candidats_v0.uid, pas sur candidats_v0.id.
    -- TODO : passer à l'id du candidat via dim_emplois__candidats quand la dimension sera disponible
    senders.beneficiary_public_id              as emplois_beneficiary_id,
    case
        when senders.sender_kind = 'emplois_prescriber' then organisations.nom
        when senders.sender_kind = 'emplois_employer' then companies.nom
    end                                        as prescriber_nom_etablissement,
    case
        when senders.sender_kind = 'emplois_prescriber' then organisations.siret_org_prescripteur
        when senders.sender_kind = 'emplois_employer' then companies.siret
    end                                        as prescriber_structure_siret
from senders
left join services
    on senders.service_uid = services.uid
left join organisations
    on
        senders.sender_kind = 'emplois_prescriber'
        and senders.sender_prescriber_organization_id = organisations.id
left join companies
    on
        senders.sender_kind = 'emplois_employer'
        and senders.sender_company_id = companies.id
left join structure_source_mapping as prescriber_structure_mapping
    on prescriber_structure_mapping.source_structure_id = 'emplois-de-linclusion--' || senders.sender_structure_uid
left join structure_source_mapping as service_structure_mapping
    on senders.structure_uid = service_structure_mapping.source_structure_id
left join data_inclusion_structures
    on service_structure_mapping.structure_id = data_inclusion_structures.structure_id
