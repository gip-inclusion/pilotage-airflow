select
    cap_campagnes.id            as id_cap_campagne,
    cap_campagnes.nom           as nom_campagne,
    cap_structures.id_structure as id_cap_structure,
    structures.id               as id_structure,
    structures.type_struct      as type,  -- noqa: references.keywords
    structures."département",
    structures."nom_département",
    structures."région",
    structures.bassin_d_emploi,
    cap_structures."état",
    cap_rep."réponse_au_contrôle",
    case
        when
            structures.active = 1
            then
                'Oui'
        else
            'Non'
    end                         as active,
    case
        when
            cap_structures."date_contrôle" is not null
            then
                'Oui'
        else
            'Non'
    end                         as controlee
from
    {{ ref('stg_structures') }} as structures
left join {{ source('emplois', 'cap_structures') }} as cap_structures
    on structures.id = cap_structures.id_structure
left join {{ source('emplois', 'cap_campagnes') }} as cap_campagnes
    on cap_structures.id_cap_campagne = cap_campagnes.id
left join {{ ref('stg_suivi_cap_reponses_controle') }} as cap_rep
    on cap_structures.id_structure = cap_rep.id_structure and cap_campagnes.id = cap_rep.id_cap_campagne
