select
    "Email",
    max("Nom Departement")   as nom_departement,
    max("Submitted at")      as "Submitted at",
    max("Nombre de Carnets") as "Nombre de Carnets"
from {{ source('mon_recap', 'Commandes_v0') }}
group by
    "Email"
