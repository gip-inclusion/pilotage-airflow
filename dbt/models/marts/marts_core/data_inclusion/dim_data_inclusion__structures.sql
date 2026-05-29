with structures as (

    select *
    from {{ ref('stg_di__structures_deduplicated') }}

),

structure_source_mapping as (

    select *
    from {{ ref('int_di__structure_source_mapping') }}

),

source_info_by_structure as (

    select
        structure_id,

        array_agg(distinct source order by source) as sources,
        count(distinct source)                     as sources_count,
        count(distinct source) > 1                 as is_multisource,

        array_agg(
            distinct source_structure_id
            order by source_structure_id
        )                                          as source_structure_ids,
        count(distinct source_structure_id)        as source_structures_count,
        count(distinct source_structure_id) > 1    as has_multiple_source_structures

    from structure_source_mapping
    group by structure_id

)

select
    structure_source_mapping.structure_id,

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

    source_info_by_structure.sources,
    source_info_by_structure.sources_count,
    source_info_by_structure.is_multisource,

    source_info_by_structure.source_structure_ids,
    source_info_by_structure.source_structures_count,
    source_info_by_structure.has_multiple_source_structures

from structures
left join structure_source_mapping
    on structures.id = structure_source_mapping.source_structure_id
left join source_info_by_structure
    on structure_source_mapping.structure_id = source_info_by_structure.structure_id
