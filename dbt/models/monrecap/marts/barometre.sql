select
    {{ pilo_star(source('monrecap','barometre_v0'), relation_alias="baro") }},
    nom_departement
from {{ source('monrecap', 'barometre_v0') }} as baro
left join {{ ref('stg_departments') }} as dpt
    on dpt.email_commande = coalesce(baro.email, baro."Votre adresse mail ?")