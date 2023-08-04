select
    {{ pilo_star(source('emplois', 'organisations'),
                 except = ['type_complet', 'ville', 'code_commune', 'région', 'nom_département'],
                 relation_alias = "organisations") }},
    initcap(organisations.ville)                                               as ville,
    coalesce(organisations.code_commune, appartenance_geo_communes.code_insee) as code_commune,
    organisations."nom_département"                                            as "nom_département",
    appartenance_geo_communes.nom_departement                                  as "nom_département_insee",
    appartenance_geo_communes.nom_region                                       as "région",
    appartenance_geo_communes.nom_zone_emploi                                  as zone_emploi,
    appartenance_geo_communes.nom_epci                                         as epci,
    organisations_libelles.libelle                                             as type_complet,
    case
        when organisations.type in ('ML', 'PE', 'CAP_EMPLOI') then 'SPE'
        when organisations.type = 'Autre' then 'Autre'
        else 'Nouveaux prescripteurs'
    end                                                                        as type_prescripteur,
    case
        when organisations."habilitée" = 1 then 'Prescripteur habilité'
        when organisations."habilitée" = 0 then 'Orienteur'
    end                                                                        as habilitation,
    case
        when organisations."habilitée" = 1 then concat('Prescripteur habilité ', organisations.type)
        when organisations."habilitée" = 0 then concat('Orienteur ', organisations.type)
    end                                                                        as type_avec_habilitation,
    case
        when organisations."habilitée" = 1 then concat('Prescripteur habilité ', organisations.type_complet)
        when organisations."habilitée" = 0 then concat('Orienteur ', organisations.type_complet)
    end                                                                        as type_complet_avec_habilitation
from {{ source('emplois', 'organisations') }} as organisations
left join {{ ref('organisations_libelles') }} as organisations_libelles
    on organisations.type = organisations_libelles.type
-- (laurine) temporary : this join is made on a cleaned version of organisations.ville
-- and based on a levenshtein distance with the clean libelle_commune of the insee table
-- (cedex and integers are remove and the levenshtein distance helps to ignore typo errors or misleading accents)
-- This is done like this as long as the organisations.ville entries are yet not cleaned
-- but it will be removed when c1 work on adressses will be finished.
left join {{ ref('stg_insee_appartenance_geo_communes') }} as appartenance_geo_communes
    on levenshtein(unaccent(regexp_replace(regexp_replace(initcap(ville), ' \d+', '', 'g'), ' Cedex ?', '')), unaccent(appartenance_geo_communes.libelle_commune)) < 1 and ltrim(organisations."département", '0') = appartenance_geo_communes.code_dept
