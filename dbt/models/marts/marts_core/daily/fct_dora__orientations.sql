with orientations_enriched as (
    select * from {{ ref('int_dora__orientations_enriched') }}
),

orientations as (
    select * from {{ ref('stg_dora__orientation') }}
),

final as (
    select
        orientations_enriched.id,
        orientations_enriched.origin_source,
        orientations_enriched.emplois_sync_uid,
        orientations_enriched.creation_date,
        orientations_enriched.processing_date,
        orientations_enriched.status,
        orientations.requirements,
        orientations.situation,
        orientations.beneficiary_contact_preferences,
        orientations.beneficiary_availability,
        orientations.orientation_reasons,
        orientations.duration_weekly_hours,
        orientations.duration_weeks,
        orientations.query_expires_at,
        orientations.last_reminder_email_sent,
        orientations.data_protection_commitment,
        orientations.is_anonymized,
        orientations_enriched.prescriber_id_dora,
        orientations_enriched.prescriber_id_emplois,
        orientations_enriched.prescriber_structure_id_di,
        orientations_enriched.prescriber_structure_id_dora,
        orientations_enriched.prescriber_structure_id_emplois,
        orientations_enriched.prescriber_structure_name,
        orientations_enriched.prescriber_structure_siret,
        orientations_enriched.oriented_service_id_di,
        orientations_enriched.oriented_service_id_dora,
        orientations_enriched.oriented_service_name,
        orientations_enriched.oriented_service_structure_name,
        orientations_enriched.oriented_service_code_commune_insee,
        orientations_enriched.emplois_beneficiary_id
    from orientations_enriched
    left join orientations
        on orientations_enriched.id = cast(orientations.id as text)
)

select *
from final
