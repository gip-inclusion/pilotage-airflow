select
    "Email"                  as email_commande,
    sum("Nombre de Carnets") as nombre_total_carnets_commandes
from {{ ref('Commandes') }}
group by
    "Email"
