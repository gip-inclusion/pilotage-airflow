select
    {{ pilo_star(source('emplois','pass_agréments'), relation_alias='pass') }},
    demandes_prolong."id_pass_agrément",
    demandes_prolong.motif,
    demandes_prolong."état",
    demandes_prolong.date_de_demande,
    s."nom_département_c1" as "département_structure",
    s."région_c1"          as "région_structure"
from
    {{ source('emplois','pass_agréments') }} as pass
left join {{ source('emplois', 'demandes_de_prolongation') }} as demandes_prolong
    on pass.id = demandes_prolong."id_pass_agrément"
left join {{ ref('structures') }} as s
    on pass.id_structure = s.id
where pass.id_candidat is not null
