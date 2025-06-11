select
    {{ pilo_star(source('monrecap','Commandes_v0')) }},
    reg.region
from {{ source('monrecap', 'Commandes_v0') }} as cmd
left join {{ ref('dep_reg_ref_emplois') }} as reg
    on cmd."Nom Departement" = reg.departement
