with communes_ranked as (
    select
        *,
        row_number() over (
            partition by code_commune_insee
            order by
                case
                    when code_commune_parente is null then 1
                    else 2
                end,
                case
                    when type_commune = 'COM' then 1
                    when type_commune = 'COMD' then 2
                    when type_commune = 'COMA' then 3
                    when type_commune = 'ARM' then 4
                    else 99
                end,
                libelle
        ) as rn
    from {{ ref('int_communes_base') }}
),

communes as (
    select
        type_commune,
        type_commune_label,
        code_commune_insee,
        code_arrondissement_insee,
        libelle        as nom_commune,
        code_commune_parente,
        parent_libelle as nom_commune_parente,
        code_region_insee,
        code_departement_insee,
        has_parent_commune,
        same_code_as_parent,
        is_arrondissement_municipal,
        region_or_departement_filled_from_parent
    from communes_ranked
    where rn = 1
),

epci_communes as (
    select
        code_commune_insee,
        code_insee_epci
    from {{ ref('stg_insee_epci_commune') }}
),

epcis as (
    select
        code_insee_epci,
        nom_epci
    from {{ ref('stg_insee_epci') }}
),

epci_resolved as (
    select
        epci_communes.code_commune_insee,
        epci_communes.code_insee_epci,
        epcis.nom_epci
    from epci_communes
    left join epcis
        on epci_communes.code_insee_epci = epcis.code_insee_epci
),

regions as (
    select
        code_region_insee,
        libelle as nom_region
    from {{ ref('stg_insee_regions') }}
),

departements as (
    select
        code_departement_insee,
        libelle as nom_departement
    from {{ ref('stg_insee_departements') }}
),

arrondissements as (
    select
        code_arrondissement_insee,
        libelle as nom_arrondissement
    from {{ ref('stg_insee_arrondissements') }}
),

zones_emploi_communes as (
    select
        code_commune_insee,
        code_zone_emploi,
        zone_emploi_partie_regionale
    from {{ ref('stg_insee_zone_emploi_commune') }}
),

zones_emploi as (
    select
        code_zone_emploi,
        zone_emploi
    from {{ ref('stg_insee_zones_emploi') }}
),

zones_emploi_resolved as (
    select
        zones_emploi_communes.code_commune_insee,
        zones_emploi_communes.code_zone_emploi,
        zones_emploi.zone_emploi,
        zones_emploi_communes.zone_emploi_partie_regionale
    from zones_emploi_communes
    left join zones_emploi
        on zones_emploi_communes.code_zone_emploi = zones_emploi.code_zone_emploi
),

clpes as (
    select
        code_commune_insee,
        code_ft_clpe,
        nom_clpe
    from {{ ref('stg_ft_clpe_commune') }}
),

codes_postaux as (
    select
        code_commune_insee,
        code_postal_principal,
        nb_codes_postaux
    from {{ ref('int_communes_code_postal') }}
)

select
    communes.code_commune_insee,
    communes.nom_commune,
    communes.type_commune,
    communes.type_commune_label,

    communes.code_region_insee,
    regions.nom_region,

    communes.code_departement_insee,
    departements.nom_departement,
    communes.code_commune_parente,

    communes.nom_commune_parente,

    communes.has_parent_commune,

    communes.same_code_as_parent,
    communes.is_arrondissement_municipal,

    communes.region_or_departement_filled_from_parent,
    concat(communes.code_departement_insee, ' - ', departements.nom_departement) as nom_departement_complet,
    coalesce(
        communes.code_arrondissement_insee,
        parent_communes.code_arrondissement_insee
    )                                                                            as code_arrondissement_insee,

    coalesce(
        arrondissement.nom_arrondissement,
        parent_arrondissement.nom_arrondissement
    )                                                                            as nom_arrondissement,
    coalesce(epci.code_insee_epci, parent_epci.code_insee_epci)                  as code_insee_epci,

    coalesce(epci.nom_epci, parent_epci.nom_epci)                                as nom_epci,

    coalesce(zone_emploi.code_zone_emploi, parent_zone_emploi.code_zone_emploi)  as code_zone_emploi,

    coalesce(zone_emploi.zone_emploi, parent_zone_emploi.zone_emploi)            as nom_zone_emploi,

    coalesce(
        zone_emploi.zone_emploi_partie_regionale,
        parent_zone_emploi.zone_emploi_partie_regionale
    )                                                                            as zone_emploi_partie_regionale,

    coalesce(clpe.code_ft_clpe, parent_clpe.code_ft_clpe)                        as code_ft_clpe,

    coalesce(clpe.nom_clpe, parent_clpe.nom_clpe)                                as nom_clpe,

    coalesce(
        codes_postaux.code_postal_principal,
        parent_codes_postaux.code_postal_principal
    )                                                                            as code_postal_principal,

    coalesce(
        codes_postaux.nb_codes_postaux,
        parent_codes_postaux.nb_codes_postaux
    )                                                                            as nb_codes_postaux,
    coalesce(
        codes_postaux.code_postal_principal is null
        and parent_codes_postaux.code_postal_principal is not null, false
    )                                                                            as code_postal_filled_from_parent,
    coalesce(
        communes.code_arrondissement_insee is null
        and parent_communes.code_arrondissement_insee is not null, false
    )                                                                            as arrondissement_filled_from_parent,
    coalesce(
        epci.code_insee_epci is null
        and parent_epci.code_insee_epci is not null, false
    )                                                                            as epci_filled_from_parent,
    coalesce(
        zone_emploi.code_zone_emploi is null
        and parent_zone_emploi.code_zone_emploi is not null, false
    )                                                                            as zone_emploi_filled_from_parent,
    coalesce(
        clpe.code_ft_clpe is null
        and parent_clpe.code_ft_clpe is not null, false
    )                                                                            as clpe_filled_from_parent

from communes
left join communes as parent_communes
    on communes.code_commune_parente = parent_communes.code_commune_insee

left join regions
    on communes.code_region_insee = regions.code_region_insee
left join departements
    on communes.code_departement_insee = departements.code_departement_insee

left join arrondissements as arrondissement
    on communes.code_arrondissement_insee = arrondissement.code_arrondissement_insee
left join arrondissements as parent_arrondissement
    on parent_communes.code_arrondissement_insee = parent_arrondissement.code_arrondissement_insee

left join epci_resolved as epci
    on communes.code_commune_insee = epci.code_commune_insee
left join epci_resolved as parent_epci
    on communes.code_commune_parente = parent_epci.code_commune_insee

left join zones_emploi_resolved as zone_emploi
    on communes.code_commune_insee = zone_emploi.code_commune_insee
left join zones_emploi_resolved as parent_zone_emploi
    on communes.code_commune_parente = parent_zone_emploi.code_commune_insee

left join clpes as clpe
    on communes.code_commune_insee = clpe.code_commune_insee
left join clpes as parent_clpe
    on communes.code_commune_parente = parent_clpe.code_commune_insee

left join codes_postaux
    on communes.code_commune_insee = codes_postaux.code_commune_insee
left join codes_postaux as parent_codes_postaux
    on communes.code_commune_parente = parent_codes_postaux.code_commune_insee
