with orientations as (
    select * from {{ source('dora', 'orientations_orientation') }}
),

final as (
    select * from orientations
    -- date discutée avec Chloé pour limiter la taille de la table
    where cast(creation_date as DATE) >= '2024-01-01'
)

select
    id,
    query_id,
    requirements,
    situation,
    beneficiary_contact_preferences,
    beneficiary_availability,
    beneficiary_attachments,
    creation_date,
    processing_date,
    status,
    prescriber_id,
    prescriber_structure_id,
    service_id,
    last_reminder_email_sent,
    query_expires_at,
    duration_weekly_hours,
    duration_weeks,
    data_protection_commitment,
    is_anonymized,
    nullif(situation_other, '')                   as situation_other,
    nullif(beneficiary_other_contact_method, '')  as beneficiary_other_contact_method,
    nullif(orientation_reasons, '')               as orientation_reasons,
    nullif(original_service_name, '')             as original_service_name,
    nullif(di_service_id, '')                     as di_service_id,
    nullif(di_service_name, '')                   as di_service_name,
    nullif(di_structure_name, '')                 as di_structure_name,
    nullif(di_service_address_line, '')           as di_service_address_line,
    nullif(beneficiary_france_travail_number, '') as beneficiary_france_travail_number
from final
