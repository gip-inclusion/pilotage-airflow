select
    "Email"                as email_commande,
    max("Nom Departement") as nom_departement
from {{ source('monrecap','Commandes_v0') }}
group by
    "Email"
