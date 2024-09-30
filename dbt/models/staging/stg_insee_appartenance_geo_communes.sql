select
    communes.code_insee,
    communes.libelle_commune,
    communes.code_region,
    regions."LIBELLE"                                                       as nom_region,
    communes.code_dept,
    departements."LIBELLE"                                                  as nom_departement,
    epci."LIBEPCI"                                                          as nom_epci,
    epci_libelle.libelle                                                    as type_epci,
    arrondissements."LIBELLE"                                               as nom_arrondissement,
    zone_emploi."LIBZE2020"                                                 as nom_zone_emploi,
    concat(lpad(communes.code_dept, 2, '0'), ' - ', departements."LIBELLE") as nom_departement_complet
from {{ ref('insee_communes') }} as communes
left join {{ ref('insee_regions') }} as regions
    on communes.code_region = regions."REG"
left join {{ ref('insee_departements') }} as departements
    on communes.code_dept = departements."DEP"
left join {{ ref('insee_epcis') }} as epci
    on communes.code_epci = epci."EPCI"
left join {{ ref('insee_epci_libelles') }} as epci_libelle
    on epci."NATURE_EPCI" = epci_libelle.code_type_epci
left join {{ ref('insee_arrondissements') }} as arrondissements
    on trim('0' from arrondissements."ARR") = communes.code_arrondissement
left join {{ ref('insee_zones_emploi') }} as zone_emploi
    on communes.code_zone_emploi = zone_emploi."ZE2020"
