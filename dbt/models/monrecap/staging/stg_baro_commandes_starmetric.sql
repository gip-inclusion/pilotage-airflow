select
    {{ pilo_star(ref('stg_commandes_starmetric'), relation_alias="cmd") }},
    {{ pilo_star(ref('stg_barometre_starmetric'), relation_alias="baro") }},
    contacts."EMAIL"                                       as email,
    contacts."STRUCTURE"                                   as structure,
    contacts."Date de dernière réponse au baromètre"::DATE as date_de_derniere_reponse_au_barometre,
    contacts."Date de dernière commande"                   as date_de_derniere_commande,
    contacts."Date de première commande"                   as date_de_premiere_commande
from {{ ref('stg_commandes_starmetric') }} as cmd
left join {{ ref('stg_barometre_starmetric') }} as baro
    on cmd.email_commande = baro.votre_adresse_mail
left join {{ ref('Contacts') }} as contacts
    on cmd.email_commande = contacts."EMAIL"
