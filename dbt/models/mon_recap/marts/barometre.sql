select
    {{ pilo_star(source('mon_recap','barometre_v0'), relation_alias="baro") }},
    dpt.nom_departement,
    dpt.region
from {{ source('mon_recap', 'barometre_v0') }} as baro
left join {{ ref('stg_departement_derniere_commandes') }} as dpt
    on dpt.email_commande = coalesce(baro.email, baro."Votre adresse mail ?")
