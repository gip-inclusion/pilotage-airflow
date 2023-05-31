select
    {{ pilo_star(ref('stg_candidats_autoprescription'), relation_alias="autopr_c") }},
    ac.total_candidats,
    s.nom_structure_complet as "nom_structure_complet"
from
    {{ ref('stg_candidats_autoprescription') }} as autopr_c
left join
    {{ ref('stg_candidats_count') }} as ac
    on autopr_c.id = ac.id
left join
    {{ ref('stg_structures') }} as s
    on autopr_c.id_structure = s.id
