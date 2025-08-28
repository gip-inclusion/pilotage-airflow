with departement_table as (
    select
        cmd."Email"                  as email_commande,
        max(cmd."Nom Departement")   as nom_departement,
        max(cmd."Submitted at")      as "Submitted at",
        max(cmd."Nombre de Carnets") as "Nombre de Carnets"
    from {{ source('mon_recap','Commandes_v0') }} as cmd
    group by
        cmd."Email"
)

select
    dpt.*,
    reg.region
from departement_table as dpt
left join {{ ref('dep_reg_ref_emplois') }} as reg
    on dpt.nom_departement = reg.departement
