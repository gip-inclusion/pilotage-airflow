select
    cmd."Email"                  as email_commande,
    reg.region,
    max(cmd."Nom Departement")   as nom_departement,
    max(cmd."Submitted at")      as "Submitted at",
    max(cmd."Nombre de Carnets") as "Nombre de Carnets"
from {{ source('monrecap','Commandes_v0') }} as cmd
left join {{ ref('dep_reg_ref_emplois') }} as reg
    on cmd."Nom Departement" = reg.departement
group by
    cmd."Email",
    reg.region
