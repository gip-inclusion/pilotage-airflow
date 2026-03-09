with communes as (
    select * from {{ ref('stg_insee_communes') }}
),

parents as (
    select
        code_commune_insee,
        code_region_insee      as parent_code_region_insee,
        code_departement_insee as parent_code_departement_insee,
        libelle                as parent_libelle
    from communes
    where code_commune_parente is null
)

select
    communes.type_commune,
    communes.type_commune_label,
    communes.code_commune_insee,

    communes.code_arrondissement_insee,
    communes.libelle,

    communes.code_commune_parente,
    parents.parent_libelle,
    coalesce(communes.code_region_insee, parents.parent_code_region_insee)           as code_region_insee,

    coalesce(communes.code_departement_insee, parents.parent_code_departement_insee) as code_departement_insee,

    coalesce(communes.code_commune_parente is not null, false)                       as has_parent_commune,

    coalesce(
        communes.code_commune_parente is not null
        and communes.code_commune_insee = communes.code_commune_parente, false
    )                                                                                as same_code_as_parent,

    coalesce(communes.type_commune = 'ARM', false)                                   as is_arrondissement_municipal,

    coalesce((
        communes.code_region_insee is null
        and parents.parent_code_region_insee is not null
    ) or (
        communes.code_departement_insee is null
        and parents.parent_code_departement_insee is not null
    ), false)                                                                        as region_or_departement_filled_from_parent

from communes
left join parents
    on communes.code_commune_parente = parents.code_commune_insee
