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
    {{ date_id_from_date("(creation_date at time zone 'UTC')::date") }}            as creation_date_id,
    processing_date,
    {{ date_id_from_date("(processing_date at time zone 'UTC')::date") }}          as processing_date_id,
    status,
    requirements,
    situation,
    beneficiary_contact_preferences,
    {{ date_id_from_date("beneficiary_availability::date") }}                      as beneficiary_availability_date_id,
    duration_weekly_hours,
    duration_weeks,
    query_expires_at,
    {{ date_id_from_date("(query_expires_at at time zone 'UTC')::date") }}         as query_expires_date_id,
    last_reminder_email_sent,
    {{ date_id_from_date("(last_reminder_email_sent at time zone 'UTC')::date") }} as last_reminder_email_sent_date_id,
    data_protection_commitment,
    is_anonymized,
    prescriber_id_dora,
    prescriber_id_emplois,
    user_kind,
    prescriber_structure_id_di,
    prescriber_structure_id_dora,
    null::uuid                                                                     as prescriber_structure_id_emplois,
    null::uuid                                                                     as prescriber_organization_id_emplois,
    prescriber_structure_name                                                      as prescriber_nom_etablissement,
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
    {{ date_id_from_date("(creation_date at time zone 'UTC')::date") }}            as creation_date_id,
    processing_date,
    {{ date_id_from_date("(processing_date at time zone 'UTC')::date") }}          as processing_date_id,
    status,
    null::varchar[]                                                                as requirements,
    null::varchar[]                                                                as situation,
    null::varchar[]                                                                as beneficiary_contact_preferences,
    {{ date_id_from_date("null::date") }}                                          as beneficiary_availability_date_id,
    duration_weekly_hours,
    duration_weeks,
    null::timestamptz                                                              as query_expires_at,
    {{ date_id_from_date("null::date") }}                                          as query_expires_date_id,
    last_reminder_email_sent,
    {{ date_id_from_date("(last_reminder_email_sent at time zone 'UTC')::date") }} as last_reminder_email_sent_date_id,
    data_protection_commitment,
    null::boolean                                                                  as is_anonymized,
    null::bigint                                                                   as prescriber_id_dora,
    prescriber_id_emplois,
    user_kind,
    prescriber_structure_id_di,
    null::uuid                                                                     as prescriber_structure_id_dora,
    prescriber_structure_id_emplois,
    prescriber_organization_id_emplois,
    prescriber_nom_etablissement,
    prescriber_structure_siret,
    oriented_service_id_di,
    null::uuid                                                                     as oriented_service_id_dora,
    oriented_service_name,
    oriented_service_structure_name,
    oriented_service_code_commune_insee,
    emplois_beneficiary_id
from emplois_orientations
