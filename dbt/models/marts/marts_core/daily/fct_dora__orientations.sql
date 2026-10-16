with dora_orientations as (
    select * from {{ ref('int_dora__orientations_enriched') }}
),

emplois_orientations as (
    select * from {{ ref('int_emplois__orientations_enriched') }}
)

select
    id,
    origin_source,
    creation_date,
    processing_date,
    status,
    requirements,
    situation,
    beneficiary_contact_preferences,
    beneficiary_availability,
    duration_weekly_hours,
    duration_weeks,
    query_expires_at,
    last_reminder_email_sent,
    data_protection_commitment,
    is_anonymized,
    prescriber_id_dora,
    prescriber_id_emplois,
    user_kind,
    prescriber_structure_id_di,
    prescriber_structure_id_dora,
    null::uuid                as prescriber_structure_id_emplois,
    null::uuid                as prescriber_organization_id_emplois,
    prescriber_structure_name as prescriber_nom_etablissement,
    prescriber_structure_siret,
    oriented_service_id_di,
    oriented_service_id_dora,
    oriented_service_name,
    oriented_service_structure_name,
    oriented_service_code_commune_insee,
    emplois_beneficiary_id
from dora_orientations

union all

select
    id,
    origin_source,
    creation_date,
    processing_date,
    status,
    null::varchar[]   as requirements,
    null::varchar[]   as situation,
    null::varchar[]   as beneficiary_contact_preferences,
    null::date        as beneficiary_availability,
    duration_weekly_hours,
    duration_weeks,
    null::timestamptz as query_expires_at,
    last_reminder_email_sent,
    data_protection_commitment,
    null::boolean     as is_anonymized,
    null::bigint      as prescriber_id_dora,
    prescriber_id_emplois,
    user_kind,
    prescriber_structure_id_di,
    null::uuid        as prescriber_structure_id_dora,
    prescriber_structure_id_emplois,
    prescriber_organization_id_emplois,
    prescriber_nom_etablissement,
    prescriber_structure_siret,
    oriented_service_id_di,
    null::uuid        as oriented_service_id_dora,
    oriented_service_name,
    oriented_service_structure_name,
    oriented_service_code_commune_insee,
    emplois_beneficiary_id
from emplois_orientations
