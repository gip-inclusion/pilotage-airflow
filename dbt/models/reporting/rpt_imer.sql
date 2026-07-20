with imer as (
    select
        id,
        date,
        user_session,
        user_kind,
        user_id,
        user_prescriber_organization_id,
        user_company_id,
        structure_id,
        source_structure_id,
        kind,
        service_id,
        source,
        orientation_id
    from {{ ref('fct_imer') }}
),

communes as (
    select
        code_commune_insee,
        nom_region,
        code_departement_insee,
        nom_departement_complet
    from {{ ref('dim_commune') }}
),

structures as (
    select
        di.structure_id                  as di_structure_id,
        di.nom                           as di_nom_structure,
        di.siret                         as di_siret_structure,
        di.code_commune_insee            as di_structure_code_commune,
        communes.nom_region              as di_structure_region,
        communes.code_departement_insee  as di_structure_code_departement,
        communes.nom_departement_complet as di_structure_nom_departement
    from {{ ref('dim_data_inclusion__structures') }} as di
    left join communes on di.code_commune_insee = communes.code_commune_insee
),

services as (
    select
        di.service_id                    as di_services_id,
        di.structure_id                  as di_services_structure_id,
        di.nom                           as di_services_nom,
        di.thematiques                   as di_services_thematiques,
        communes.nom_region              as di_services_region,
        communes.code_departement_insee  as di_services_code_departement,
        communes.nom_departement_complet as di_services_nom_departement
    from {{ ref('dim_data_inclusion__services') }} as di
    left join communes on di.code_commune_insee = communes.code_commune_insee
),

organization as (
    select
        id                       as organization_id,
        nom                      as organization_nom,
        type                     as organization_type,
        "région"                 as organization_region,
        numero_departement_insee as organization_numero_departement,
        "nom_département_insee"  as organization_nom_departement
    from {{ ref('organisations') }}
),

company as (
    select
        id                   as company_id,
        nom                  as company_nom,
        type                 as company_type,
        "région_c1"          as company_region,
        "département_c1"     as company_numero_departement,
        "nom_département_c1" as company_nom_departement,
        source               as company_source
    from {{ ref('structures') }}
)

select
    imer.id,
    imer.date,
    imer.user_session,
    imer.user_kind,
    imer.user_id,
    imer.user_prescriber_organization_id,
    imer.user_company_id,
    imer.structure_id,
    imer.source_structure_id,
    imer.kind,
    imer.service_id,
    imer.source,
    imer.orientation_id,
    structures.di_nom_structure,
    structures.di_siret_structure,
    structures.di_structure_code_commune,
    structures.di_structure_region,
    structures.di_structure_code_departement,
    structures.di_structure_nom_departement,
    services.di_services_nom,
    services.di_services_thematiques,
    services.di_services_region,
    services.di_services_code_departement,
    services.di_services_nom_departement,
    organization.organization_nom,
    organization.organization_type,
    organization.organization_region,
    organization.organization_numero_departement,
    organization.organization_nom_departement,
    company.company_nom,
    company.company_type,
    company.company_region,
    company.company_numero_departement,
    company.company_nom_departement,
    company.company_source
from imer
left join structures on imer.structure_id = structures.di_structure_id
left join services on imer.service_id = services.di_services_id
left join organization on imer.user_prescriber_organization_id = organization.organization_id
left join company on imer.user_company_id = company.company_id
