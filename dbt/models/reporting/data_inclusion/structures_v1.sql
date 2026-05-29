with structures as (

    select *
    from {{ ref('data_inclusion__structures_sans_dedupliquer') }}

),

communes as (

    select *
    from {{ ref('dim_commune') }}

)

select
    structures.source_structure_id as id,
    structures.source,

    structures.nom,
    structures.description,
    structures.lien_source,
    structures.date_maj,
    structures.siret,

    structures.code_commune_insee,
    communes.nom_commune           as commune,
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
    structures.doublons

from structures
left join communes
    on structures.code_commune_insee = communes.code_commune_insee
where source != 'soliguide'
