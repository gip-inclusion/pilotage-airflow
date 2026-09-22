select distinct on ("Email", "Submitted at")
{{ pilo_star(source('monrecap','Commandes_v0'), relation_alias ="cmd") }}
from {{ ref('stg_departement_derniere_commandes') }} as last_cmd
inner join {{ source('monrecap','Commandes_v0') }} as cmd
    on
        last_cmd.email_commande = cmd."Email"
        and last_cmd."Submitted at" = cmd."Submitted at"
