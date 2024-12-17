select
    {{ pilo_star(source('monrecap','Contacts_v0'), relation_alias="contacts") }},
    nom_departement
from {{ source('monrecap', 'Contacts_v0') }} as contacts
left join {{ ref('stg_departments') }} as dpt
    on dpt.email_commande = contacts."EMAIL"
