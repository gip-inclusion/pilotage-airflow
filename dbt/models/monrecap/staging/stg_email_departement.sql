select
    "Email"           as email_commande,
    "Nom Departement" as nom_departement
from {{ ref('Commandes') }}
group by
    "Email",
    "Nom Departement"
