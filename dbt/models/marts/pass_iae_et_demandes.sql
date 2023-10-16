select
    {{ pilo_star(source('emplois','pass_agréments'), relation_alias='pass') }},
    demandes_prolong."id_pass_agrément",
    demandes_prolong.motif,
    demandes_prolong."état",
    demandes_prolong.date_de_demande
from
    {{ source('emplois','pass_agréments') }} as pass
left join {{ source('emplois', 'demandes_de_prolongation') }} as demandes_prolong
    on pass.id = demandes_prolong."id_pass_agrément"
where pass.id_candidat is not null
