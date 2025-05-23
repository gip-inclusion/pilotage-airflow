select
    {{ pilo_star(ref('stg_candidatures_autoprescription'), relation_alias='autopr_all') }},
    s.siret,
    s.active,
    s.ville,
    s.nom_structure_complet
from
    {{ ref('stg_candidatures_autoprescription') }} as autopr_all
left join
    {{ ref('stg_structures') }} as s
    on autopr_all.id_structure = s.id
