with commandeurs as (
    select * from {{ ref('stg_contacts_commandeurs') }}
),

non_commandeurs as (
    select * from {{ ref('stg_contacts_non_commandeurs') }}
),

contacts as (
    select
        "EMAIL",
        "STRUCTURE",
        "Contact",
        "Type de contact",
        "Type de Structure",
        "Code dpt",
        "Date de dernière commande",
        "Date de première commande"
    from commandeurs

    union all

    select
        "EMAIL",
        "STRUCTURE",
        "Contact",
        "Type de contact",
        "Type de Structure",
        "Code dpt",
        "Date de dernière commande",
        "Date de première commande"
    from non_commandeurs
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['contacts."EMAIL"']) }} as id,
        'MonRécap'                                                   as source,
        contacts."Date de dernière commande"                         as date_derniere_connexion,
        contacts."Contact"                                           as prenom,
        contacts."Contact"                                           as nom,
        contacts."Type de contact"                                   as type_utilisateur,
        contacts."Date de première commande"                         as date_inscription,
        contacts."EMAIL"                                             as email,
        'NA'                                                         as type_utilisateur_detail,
        contacts."STRUCTURE"                                         as nom_structure,
        contacts."Type de Structure"                                 as type_structure,
        contacts."Code dpt"                                          as departement_structure
    from contacts
    where
        contacts."Code dpt" not in ('To', 'LI', 'li', 'Do', 'Ch', 'AN', '[N') --legacy from Pierre's work
        or contacts."Code dpt" is null
        and contacts."EMAIL" is not null
)

select * from final
