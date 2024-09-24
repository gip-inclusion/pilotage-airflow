select
    "Email"           as email_commande,
    "Nom Departement" as nom_departement
from {{ source('monrecap', 'Commandes') }}
group by
    "Email",
    "Nom Departement"
