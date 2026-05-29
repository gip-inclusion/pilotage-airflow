with structures as (
    select * from {{ ref('stg_di__structures') }}
),

structure_source_mapping as (

    select *
    from {{ ref('int_di__structure_source_mapping') }}

)

select
    structures.id         as source_structure_id,
    structure_source_mapping.structure_id,
    structures.source,

    structures.nom,
    structures.description,
    structures.lien_source,
    structures.date_maj,
    structures.siret,

    structures.code_insee as code_commune_insee,
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
left join structure_source_mapping
    on structures.id = structure_source_mapping.source_structure_id
