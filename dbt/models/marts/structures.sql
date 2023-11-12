select
    {{ pilo_star(source('emplois','structures_v0'), relation_alias='s') }},
    grp_strct.groupe as categorie_structure
from
    {{ source('emplois','structures_v0') }} as s
left join
    {{ ref('groupes_structures') }} as grp_strct
    on grp_strct.structure = s.type
