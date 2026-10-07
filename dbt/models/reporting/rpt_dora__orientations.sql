with orientations as (
    select * from {{ ref('fct_dora__orientations') }}
),

communes as (
    select
        code_commune_insee,
        nom_commune,
        code_region_insee,
        nom_region,
        code_departement_insee,
        nom_departement,
        nom_departement_complet
    from {{ ref('dim_commune') }}
)

select
    orientations.id,
    orientations.origin_source,
    orientations.emplois_sync_uid,
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
    orientations.prescriber_id_dora,
    orientations.prescriber_id_emplois,
    orientations.user_kind,
    orientations.prescriber_structure_id_di,
    orientations.prescriber_structure_id_dora,
    orientations.prescriber_structure_id_emplois,
    orientations.prescriber_structure_name,
    orientations.prescriber_structure_siret,
    orientations.oriented_service_id_di,
    orientations.oriented_service_id_dora,
    orientations.oriented_service_name,
    orientations.oriented_service_structure_name,
    orientations.oriented_service_code_commune_insee,
    orientations.emplois_beneficiary_id,
    communes.nom_commune             as oriented_service_nom_commune,
    communes.code_region_insee       as oriented_service_code_region_insee,
    communes.nom_region              as oriented_service_nom_region,
    communes.code_departement_insee  as oriented_service_code_departement_insee,
    communes.nom_departement         as oriented_service_nom_departement,
    communes.nom_departement_complet as oriented_service_nom_departement_complet
from orientations
left join communes
    on orientations.oriented_service_code_commune_insee = communes.code_commune_insee
