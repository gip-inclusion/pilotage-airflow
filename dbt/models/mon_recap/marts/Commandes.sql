select
    {{ pilo_star(source('mon_recap','Commandes_v0'), except =["declaratif quels freins"]) }},
    cmd."declaratif quels freins"::text as "declaratif quels freins",
    reg.region
from {{ source('mon_recap', 'Commandes_v0') }} as cmd
left join {{ ref('dep_reg_ref_emplois') }} as reg
    on cmd."Nom Departement" = reg.departement
