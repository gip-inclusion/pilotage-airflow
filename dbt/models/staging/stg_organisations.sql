select
    {{ pilo_star(source('emplois', 'organisations_v0'),
                 except = ['siret', 'nom_département', 'type_complet', 'ville', 'code_commune', 'région', 'nom_département'],
                 relation_alias = "organisations") }},
    -- récupérer la ville insee si le croisement insee a bien pu se faire
    -- pour les 492 villes restantes, récupérer la ville des emplois
    organisations."nom_département",
    organisations.siret                                                               as siret_org_prescripteur,
    organisations."nom_département"                                                   as dept_org,
    -- les deux colonnes suivantes sont en doublons le temps de vérifier les branchements de filtre sur metabase
    organisations."région"                                                            as "région_org",
    organisations."région",
    /*bien mettre nom département et pas département */
    appartenance_geo_communes.nom_departement                                         as "nom_département_insee",
    appartenance_geo_communes.nom_zone_emploi                                         as zone_emploi,
    appartenance_geo_communes.nom_epci                                                as epci,
    organisations_libelles.label                                                      as type_complet,
    organisations_libelles.code                                                       as type_org,
    coalesce(appartenance_geo_communes.libelle_commune, initcap(organisations.ville)) as ville,
    coalesce(organisations.code_commune, appartenance_geo_communes.code_insee)        as code_commune,
    case
        when organisations.type in ('ML', 'PE', 'CAP_EMPLOI') then 'SPE'
        when organisations.type in ('DEPT', 'ODC') then 'Département'
        when organisations.type = 'Autre' then 'Autre'
        else 'Nouveaux prescripteurs'
    end                                                                               as type_prescripteur,
    case
        when organisations."habilitée" = 1 then 'Prescripteur habilité'
        when organisations."habilitée" = 0 then 'Orienteur'
    end                                                                               as habilitation,
    case
        when organisations."habilitée" = 1 then concat('Prescripteur habilité ', organisations.type)
        when organisations."habilitée" = 0 then concat('Orienteur ', organisations.type)
    end                                                                               as type_avec_habilitation,
    case
        when organisations."habilitée" = 1 then concat('Prescripteur habilité ', organisations.type_complet)
        when organisations."habilitée" = 0 then concat('Orienteur ', organisations.type_complet)
    end                                                                               as type_complet_avec_habilitation
from {{ source('emplois', 'organisations_v0') }} as organisations
left join {{ source('emplois','c1_ref_type_prescripteur') }} as organisations_libelles
    on organisations.type = organisations_libelles.code
left join {{ ref('stg_insee_appartenance_geo_communes') }} as appartenance_geo_communes
    on organisations.code_commune = ltrim(appartenance_geo_communes.code_insee, '0')
