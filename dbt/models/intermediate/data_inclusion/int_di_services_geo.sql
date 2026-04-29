with services as (
    select
        {{ dbt_utils.star(ref('stg_di_services')) }}
    from {{ ref('stg_di_services') }}
),

geo as (
    select
        {{ dbt_utils.star(ref('dim_commune')) }}
    from {{ ref('dim_commune') }}
),

geo_by_postal as (
    select distinct on (code_postal_principal)
        {{ dbt_utils.star(ref('dim_commune')) }}
    from {{ ref('dim_commune') }}
    order by code_postal_principal, code_commune_insee
)

select
    services.*,
    coalesce(geo.code_commune_insee)                                             as code_commune_insee,
    coalesce(geo.nom_commune)                                                    as nom_commune,
    coalesce(geo.type_commune)                                                   as type_commune,
    coalesce(geo.type_commune_label)                                             as type_commune_label,
    coalesce(geo.code_region_insee, geo_by_postal.code_region_insee)             as code_region_insee,
    coalesce(geo.nom_region, geo_by_postal.nom_region)                           as nom_region,
    coalesce(geo.code_departement_insee, geo_by_postal.code_departement_insee)   as code_departement_insee,
    coalesce(geo.nom_departement, geo_by_postal.nom_departement)                 as nom_departement,
    coalesce(geo.code_commune_parente)                                           as code_commune_parente,
    coalesce(geo.nom_commune_parente)                                            as nom_commune_parente,
    coalesce(geo.has_parent_commune)                                             as has_parent_commune,
    coalesce(geo.same_code_as_parent)                                            as same_code_as_parent,
    coalesce(geo.nom_departement_complet, geo_by_postal.nom_departement_complet) as nom_departement_complet,
    coalesce(geo.code_arrondissement_insee)                                      as code_arrondissement_insee,
    coalesce(geo.nom_arrondissement)                                             as nom_arrondissement,
    coalesce(geo.code_insee_epci)                                                as code_insee_epci,
    coalesce(geo.nom_epci)                                                       as nom_epci,
    coalesce(geo.code_zone_emploi)                                               as code_zone_emploi,
    coalesce(geo.nom_zone_emploi)                                                as nom_zone_emploi,
    coalesce(geo.zone_emploi_partie_regionale)                                   as zone_emploi_partie_regionale,
    coalesce(geo.code_ft_clpe)                                                   as code_ft_clpe,
    coalesce(geo.nom_clpe)                                                       as nom_clpe
from services
left join geo on services.code_insee = geo.code_commune_insee
left join geo_by_postal on services.code_postal = geo_by_postal.code_postal_principal
