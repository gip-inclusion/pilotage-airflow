with mails as (
    select *
    from {{ ref('fct_emplois__inbound_parsing_mer_email') }}
),

imer as (
    select *
    from {{ ref('fct_dora__imer') }}
),

services as (
    select *
    from {{ ref('dim_emplois__services') }}
),

structure_source_mapping as (
    select
        source_structure_id,
        structure_id
    from {{ ref('int_di__structure_source_mapping') }}
),

structures as (
    select
        structure_id,
        nom
    from {{ ref('dim_data_inclusion__structures') }}
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
    mails.id,
    mails.imer_id,
    mails.service_uid_di,
    mails.sent_at,
    mails.sent_date,
    mails.answered_at,
    mails.is_answered,
    mails.response_time_days,
    mails.date_mise_a_jour_metabase,
    imer.source_imer_id,
    imer.user_kind,
    imer.user_id,
    imer.user_prescriber_etablissement,
    imer.user_prescriber_etablissement_type,
    services.id                        as service_id,
    services.structure_uid_di          as service_structure_uid_di,
    structures.nom                     as service_structure_name,
    services.source                    as service_source,
    services.source_link               as service_source_link,
    services.name                      as service_name,
    services.description               as service_description,
    services.duration_weekly_hours     as service_duration_weekly_hours,
    services.duration_weeks            as service_duration_weeks,
    services.code_insee                as service_code_insee,
    communes.nom_commune               as service_nom_commune,
    communes.code_region_insee         as service_code_region_insee,
    communes.nom_region                as service_nom_region,
    communes.code_departement_insee    as service_code_departement_insee,
    communes.nom_departement           as service_nom_departement,
    communes.nom_departement_complet   as service_nom_departement_complet,
    services.is_active                 as service_is_active,
    services.date_mise_a_jour_metabase as service_date_mise_a_jour_metabase
from mails
left join imer
    on mails.imer_id = imer.id
left join services
    on mails.service_uid_di = services.service_uid_di
left join structure_source_mapping
    on services.structure_uid_di = structure_source_mapping.source_structure_id
left join structures
    on structure_source_mapping.structure_id = structures.structure_id
left join communes
    on services.code_insee = communes.code_commune_insee
