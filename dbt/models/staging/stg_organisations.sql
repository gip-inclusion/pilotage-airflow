select
    {{ pilo_star(source('emplois', 'organisations_v0'),
                 except = ['siret', 'nom_département', 'type_complet', 'ville', 'code_commune', 'région'],
                 relation_alias = "organisations") }},

    -- récupérer la ville insee si le croisement insee a bien pu se faire
    -- pour les 492 villes restantes, récupérer la ville des emplois
    organisations."nom_département",
    organisations.siret                                             as siret_org_prescripteur,
    organisations."nom_département"                                 as dept_org,
    organisations."département"                                     as "num_département_org",

    -- région_org conserve la valeur source historique.
    -- région est recalculée depuis dim_commune et doit être utilisée comme référence géographique.
    organisations."région"                                          as "région_org",
    dim_commune.nom_region                                          as "région",

    -- nom_département_insee est recalculé depuis dim_commune
    -- et doit être utilisé comme référence géographique.
    dim_commune.nom_departement                                     as "nom_département_insee",
    dim_commune.nom_departement_complet,
    dim_commune.nom_arrondissement,
    dim_commune.nom_zone_emploi                                     as zone_emploi,
    dim_commune.nom_epci                                            as epci,

    organisations_libelles.label                                    as type_complet,
    organisations_libelles.code                                     as type_org,

    organisations.code_commune,
    coalesce(dim_commune.nom_commune, initcap(organisations.ville)) as ville,

    case
        when organisations.type in ('ML', 'FT', 'CAP_EMPLOI') then 'SPE'
        when organisations.type in ('DEPT', 'ODC') then 'Département'
        when organisations.type = 'Autre' then 'Autre'
        else 'Nouveaux prescripteurs'
    end                                                             as type_prescripteur,

    case
        when organisations."habilitée" = 1 then 'Prescripteur habilité'
        when organisations."habilitée" = 0 then 'Orienteur'
    end                                                             as habilitation,

    case
        when organisations."habilitée" = 1 then concat('Prescripteur habilité ', organisations.type)
        when organisations."habilitée" = 0 then concat('Orienteur ', organisations.type)
    end                                                             as type_avec_habilitation,

    case
        when organisations."habilitée" = 1 then concat('Prescripteur habilité ', organisations.type_complet)
        when organisations."habilitée" = 0 then concat('Orienteur ', organisations.type_complet)
    end                                                             as type_complet_avec_habilitation

from {{ source('emplois', 'organisations_v0') }} as organisations

left join {{ source('emplois','c1_ref_type_prescripteur') }} as organisations_libelles
    on organisations.type = organisations_libelles.code

left join {{ ref('dim_commune') }}
    on organisations.code_commune = dim_commune.code_commune_insee
