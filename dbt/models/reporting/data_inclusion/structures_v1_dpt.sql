with structures as (

    select *
    from {{ ref('structures_v1') }}

),

communes as (

    select *
    from {{ ref('dim_commune') }}

)

select
    structures.id,
    structures.source,

    structures.nom,
    structures.description,
    structures.lien_source,
    structures.date_maj,
    structures.siret,

    structures.code_commune_insee,
    structures.commune,
    structures.code_postal,
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
    structures.doublons,

    communes.nom_departement,
    communes.code_departement_insee as code_dept

from structures
left join communes
    on structures.code_commune_insee = communes.code_commune_insee
