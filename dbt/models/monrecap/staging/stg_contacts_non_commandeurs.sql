select
    cnm."EMAIL",
    cmd."Nom de votre structure"                                        as "STRUCTURE",
    cmd.reseau::INT                                                     as "Réseau",
    cnm."LIEN_TALLY",
    null::DATE                                                          as "Date jour",
    cnm."date de la dernière commande"                                  as "Date de dernière commande",
    null                                                                as "Tel",
    null                                                                as "Fonction",
    null                                                                as "Contact",
    cnm."LIEN_BARO",
    cmd."Code Postal",
    cnm."Nombre d'envois du baro"                                       as "Nombre d'envois du baromètre",
    cmd."Type de structure",
    cnm."Date de première commande",
    null::DATE                                                          as "Date envoi mail Relance J+71",
    null                                                                as "Fonction (recatégorisée)",
    cmd."Votre structure appartient à une structure, un réseau ou un l" as "Structure-réseau-label",
    null                                                                as "Code dpt",
    null                                                                as "Objectifs de réponses enquêtes",
    null                                                                as "Qui ?",
    null                                                                as "date de l'échange",
    null                                                                as "Vague d'envoi mail enquête usagers",
    null                                                                as "A relancer",
    null                                                                as "Envoyeur",
    null                                                                as "Nbre de réponses à l'enquête au 25/04",
    cnm."Date de la dernière réponse au baromètre",
    cnm."Utilise le carnet ?",
    null::DATE                                                          as "Date envoi mail merci",
    null                                                                as "Réticences",
    null                                                                as "Estimation par la structure",
    null                                                                as "Infos complémentaires",
    null                                                                as "Suivi appels",
    null                                                                as "Mail CC",
    cnm."Type de contact",
    null                                                                as "Organisation",
    cnm."Commandeur référent",
    cnm."Commandes",
    cnm."date de la dernière commande",
    cmd."Adresse",
    cmd."Ville",
    cmd."declaratif autonomie",
    cmd."declaratif autre outil",
    cmd."declaratif plusieurs freins",
    cmd."declaratif quels freins",
    cmd."Votre structure appartient à une structure, un réseau ou un l",
    dpt.nom_departement,
    dpt.region,
    null::BOOLEAN                                                       as "Commande passée",
    null::INT                                                           as "Nombre de carnets commandés Total",
    null::INT                                                           as "Nombre de commandes total",
    null::BOOLEAN                                                       as "Relance baro 30j J+71",
    null::FLOAT                                                         as "Total commandes TTC",
    null::DATE                                                          as "Date de dernière commande auto",
    null::BOOLEAN                                                       as "Kit appropriation envoyé",
    null::BOOLEAN                                                       as "Manuel",
    null                                                                as "type pour baromètre",
    null::FLOAT                                                         as "Nombre de réponses au baromètre",
    null::BOOLEAN                                                       as "Envoi mail merci",
    null::BOOLEAN                                                       as "Logement ?",
    null::BOOLEAN                                                       as "Conseil Consultatif",
    null::BOOLEAN                                                       as "Offre découverte",
    null::BOOLEAN                                                       as "Offre payante (devis validé)",
    (cnm."date du dernier envoi de baro")::DATE                         as "Date d'envoi du dernier baromètre",
    (cnm."date du premier envoi de baro")::DATE                         as "Date d'envoi du premier Baromètre",
    cmd."Source",
    null                                                                as "recup fonction",
    null                                                                as "Secteur"
from {{ source('monrecap', 'contacts_non_commandeurs_v0') }} as cnm
left join {{ ref('stg_commandes_max') }} as cmd
    on cnm.submission_id = cmd."Submission ID"
left join {{ ref('stg_departement_derniere_commandes') }} as dpt
    on cnm."Commandeur référent" = dpt.email_commande
-- A contact who did not place an order cannot have the filed email and commandeur referent completed
-- this condition removes the persons that placed an order and made a mistake while filling the form
where cnm."EMAIL" != cnm."Commandeur référent"
