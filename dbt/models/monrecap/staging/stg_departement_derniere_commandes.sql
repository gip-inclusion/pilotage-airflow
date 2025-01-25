select
    "Email"                  as email_commande,
    max("Nom Departement")   as nom_departement,
    max("Submitted at")      as "Submitted at",
    max("Nombre de Carnets") as "Nombre de Carnets"
from {{ source('monrecap','Commandes_v0') }}
group by
    "Email"
