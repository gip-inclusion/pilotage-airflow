with imer as (
    select
        id,
        origin_source,
        source_imer_id,
        date,
        user_session,
        user_kind,
        user_id,
        user_prescriber_organization_id,
        user_company_id,
        target_structure_source_id,
        target_di_structure_id,
        kind,
        service_id,
        source,
        orientation_id
    from {{ ref('fct_dora__imer') }}
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

dora_structures as (
    select
        dora.structure_id_jointure_di    as dora_structure_id_jointure_di,
        dora.name                        as dora_structure_nom,
        dora.siret                       as dora_structure_siret,
        dora.city_code                   as dora_structure_code_commune,
        communes.nom_region              as dora_structure_region,
        communes.nom_departement_complet as dora_structure_nom_departement,
        coalesce(
            communes.code_departement_insee,
            dora.department
        )                                as dora_structure_code_departement
    from {{ ref('dim_dora__structures') }} as dora
    left join communes on dora.city_code = communes.code_commune_insee
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
),

imer_enriched as (
    select
        imer.id,
        imer.origin_source,
        imer.source_imer_id,
        imer.date,
        imer.user_session,
        imer.user_kind,
        imer.user_id,
        imer.user_prescriber_organization_id,
        imer.user_company_id,
        imer.target_structure_source_id,
        imer.target_di_structure_id,
        imer.kind,
        imer.service_id,
        imer.source,
        imer.orientation_id,
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
        company.company_source,
        coalesce(
            target_di_structures.di_nom_structure,
            service_di_structures.di_nom_structure,
            dora_structures.dora_structure_nom
        ) as target_structure_nom,
        coalesce(
            target_di_structures.di_siret_structure,
            service_di_structures.di_siret_structure,
            dora_structures.dora_structure_siret
        ) as target_structure_siret,
        coalesce(
            target_di_structures.di_structure_code_commune,
            service_di_structures.di_structure_code_commune,
            dora_structures.dora_structure_code_commune
        ) as target_structure_code_commune,
        coalesce(
            target_di_structures.di_structure_region,
            service_di_structures.di_structure_region,
            dora_structures.dora_structure_region
        ) as target_structure_region,
        coalesce(
            target_di_structures.di_structure_code_departement,
            service_di_structures.di_structure_code_departement,
            dora_structures.dora_structure_code_departement
        ) as target_structure_code_departement,
        coalesce(
            target_di_structures.di_structure_nom_departement,
            service_di_structures.di_structure_nom_departement,
            dora_structures.dora_structure_nom_departement
        ) as target_structure_nom_departement
    from imer
    left join services on imer.service_id = services.di_services_id
    left join structures as target_di_structures
        on imer.target_di_structure_id = target_di_structures.di_structure_id
    left join structures as service_di_structures
        on services.di_services_structure_id = service_di_structures.di_structure_id
    left join dora_structures
        on imer.target_structure_source_id = dora_structures.dora_structure_id_jointure_di
    left join organization on imer.user_prescriber_organization_id = organization.organization_id
    left join company on imer.user_company_id = company.company_id
)

select
    id,
    origin_source,
    source_imer_id,
    date,
    user_session,
    user_kind,
    user_id,
    user_prescriber_organization_id,
    user_company_id,
    target_structure_source_id,
    target_di_structure_id,
    kind,
    service_id,
    source,
    orientation_id,
    target_structure_nom,
    target_structure_siret,
    target_structure_code_commune,
    target_structure_region,
    target_structure_code_departement,
    target_structure_nom_departement,
    di_services_nom,
    di_services_thematiques,
    di_services_region,
    di_services_code_departement,
    di_services_nom_departement,
    organization_nom,
    organization_type,
    organization_region,
    organization_numero_departement,
    organization_nom_departement,
    company_nom,
    company_type,
    company_region,
    company_numero_departement,
    company_nom_departement,
    company_source
from imer_enriched
