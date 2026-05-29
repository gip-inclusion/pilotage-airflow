with structures as (

    select *
    from {{ ref('dim_data_inclusion__structures') }}

),

services as (

    select *
    from {{ ref('services_thematiques_public_accueil_v1') }}

),

communes as (

    select *
    from {{ ref('dim_commune') }}

)

select
    structures.structure_id,
    structures.sources,
    structures.sources_count,
    structures.is_multisource,

    structures.nom,
    structures.date_maj,
    structures.description,
    structures.lien_source,
    structures.siret,
    structures.code_postal,
    structures.code_commune_insee,
    structures.adresse,
    structures.complement_adresse,
    structures.longitude,
    structures.latitude,
    structures.telephone,
    structures.courriel,
    structures.site_web,
    structures.horaires_accueil,
    structures.accessibilite_lieu,
    structures.reseaux_porteurs,
    structures.adresse_certifiee,
    structures.score_qualite,

    reseau_porteur,

    services.service_thematique,
    services.service_public,
    services.service_mode_accueil,

    communes.nom_commune as commune,
    communes.nom_region,
    communes.code_region_insee,
    communes.nom_departement,
    communes.code_departement_insee

from structures
left join lateral unnest(structures.reseaux_porteurs) as reseau_porteur on true
left join services
    on structures.structure_id = services.structure_id
left join communes
    on structures.code_commune_insee = communes.code_commune_insee

where exists (
    select 1
    from unnest(structures.sources) as structure_source
    where structure_source != 'soliguide'
)
