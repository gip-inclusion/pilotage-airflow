select
    communes.code_insee       as code_insee,
    communes.libelle_commune  as libelle_commune,
    communes.code_region      as code_region,
    regions."LIBELLE"         as nom_region,
    communes.code_dept        as code_dept,
    departements."LIBELLE"    as nom_departement,
    epci."LIBEPCI"            as nom_epci,
    epci_libelle."libelle"    as type_epci,
    arrondissements."LIBELLE" as nom_arrondissement,
    zone_emploi."LIBZE2020"   as nom_zone_emploi
from {{ ref('insee_communes') }} as communes
left join {{ ref('insee_regions') }} as regions
    on regions."REG" = communes.code_region
left join {{ ref('insee_departements') }} as departements
    on departements."DEP" = communes.code_dept
left join {{ ref('insee_epcis') }} as epci
    on epci."EPCI" = communes.code_epci
left join {{ ref('insee_epci_libelles') }} as epci_libelle
    on epci."NATURE_EPCI" = epci_libelle.code_type_epci
left join {{ ref('insee_arrondissements') }} as arrondissements
    on trim('0' from arrondissements."ARR") = communes.code_arrondissement
left join {{ ref('insee_zones_emploi') }} as zone_emploi
    on zone_emploi."ZE2020" = communes.code_zone_emploi