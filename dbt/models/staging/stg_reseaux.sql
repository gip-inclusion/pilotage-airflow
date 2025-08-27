select
    {{ pilo_star(source('oneshot', 'reseau_iae_adherents')) }},
    s.id as id_structure,
    rid.id_institution
from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
left join {{ ref('reseau_iae_ids') }} as rid
    on ria."Réseau IAE" = rid.nom
left join {{ ref('structures') }} as s
    on ria."SIRET" = s.siret
