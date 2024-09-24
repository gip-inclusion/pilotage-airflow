select
    "Email"                  as email_commande,
    sum("Nombre de Carnets") as nombre_total_carnets_commandes
from {{ source('monrecap', 'Commandes') }}
group by
    "Email"
