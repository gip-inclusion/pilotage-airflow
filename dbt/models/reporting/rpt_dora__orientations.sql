with orientations as (
    select * from {{ ref('fct_dora__orientations') }}
),

calendrier as (
    select
        date_id,
        date_jour,
        date_debut_semaine,
        date_debut_mois
    from {{ ref('dim_date') }}
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
    calendrier.date_jour             as date_de_creation,
    calendrier.date_debut_semaine    as semaine_de_creation,
    calendrier.date_debut_mois       as mois_de_creation,
    orientations.id,
    orientations.origin_source,
    orientations.creation_date,
    orientations.processing_date,
    orientations.processing_date_id,
    orientations.status,
    orientations.requirements,
    orientations.situation,
    orientations.beneficiary_contact_preferences,
    orientations.beneficiary_availability_date_id,
    orientations.duration_weekly_hours,
    orientations.duration_weeks,
    orientations.query_expires_at,
    orientations.query_expires_date_id,
    orientations.last_reminder_email_sent,
    orientations.last_reminder_email_sent_date_id,
    orientations.data_protection_commitment,
    orientations.is_anonymized,
    orientations.prescriber_id_dora,
    orientations.prescriber_id_emplois,
    orientations.user_kind,
    orientations.prescriber_structure_id_di,
    orientations.prescriber_structure_id_dora,
    orientations.prescriber_structure_id_emplois,
    orientations.prescriber_organization_id_emplois,
    orientations.prescriber_nom_etablissement,
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
left join calendrier
    on orientations.creation_date_id = calendrier.date_id
