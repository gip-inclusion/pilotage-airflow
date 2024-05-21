select
    criteres."nom"        as "nom_critère",
    criteres."id"         as "id_critère",
    criteres."niveau"     as "niveau_critère",
    camp."nom"            as "nom_campagne",
    structs."id"          as "id_structure",
    structs."nom"         as "nom_structure",
    structs."nom_structure_complet",
    structs."département",
    structs."nom_département",
    structs."région"      as "nom_région",
    structs.bassin_d_emploi,
    structs."type_struct" as "type_structure",
    case
        when
            cap_criteres."état" = 'ACCEPTED'
            then
                'Accepté'
        when
            cap_criteres."état" = 'REFUSED_2'
            then
                'Refusé'
        when
            cap_criteres."état" = 'REFUSED'
            then
                'Refusé'
        when
            cap_criteres."état" = 'PENDING' and cap_criteres."date_transmission" is not null
            then
                'Non vérifié'
        when
            cap_criteres."état" = 'PENDING' and cap_criteres."date_transmission" is null
            then
                'Non transmis'
    end                   as "état"
from
    {{ source('emplois', 'cap_critères_iae') }} as cap_criteres
left join {{ source('emplois', 'critères_iae') }} as criteres on cap_criteres."id_critère_iae" = criteres."id"
left join {{ source('emplois', 'cap_candidatures') }} as candidatures on cap_criteres."id_cap_candidature" = candidatures."id"
left join {{ source('emplois', 'cap_structures') }} as cap_structs on candidatures."id_cap_structure" = cap_structs."id"
left join {{ ref('stg_structures') }} as structs on cap_structs."id_structure" = structs."id"
left join {{ source('emplois', 'cap_campagnes') }} as camp on cap_structs."id_cap_campagne" = camp."id"
