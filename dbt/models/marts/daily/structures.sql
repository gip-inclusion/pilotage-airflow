select
    {{ pilo_star(source('emplois','structures_v0'), except=['source'], relation_alias='s') }},
    grp_strct.groupe as categorie_structure,
    case
        when type = 'OPCS' and source = 'Utilisateur (Antenne)' then 'Utilisateur (OPCS)'
        when type = 'OPCS' and source = 'Staff Itou' then 'Staff Itou (OPCS)'
        else source
    end              as source
from
    {{ source('emplois','structures_v0') }} as s
left join
    {{ ref('groupes_structures') }} as grp_strct
    on grp_strct.structure = s.type
