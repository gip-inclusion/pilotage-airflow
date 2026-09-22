with dora as (
    select
        id::text as id,
        source,
        date_derniere_connexion,
        prenom,
        nom,
        type_utilisateur,
        date_inscription,
        email,
        admin,
        type_utilisateur_detail,
        departement_structure,
        type_structure,
        nom_structure
    from {{ ref('int_dora__tous_les_pros') }}
),

emplois as (
    select
        id::text as id,
        source,
        date_derniere_connexion,
        prenom,
        nom,
        type_utilisateur,
        date_inscription,
        email,
        admin,
        type_utilisateur_detail,
        departement_structure,
        type_structure,
        nom_structure
    from {{ ref('int_emplois__tous_les_pros') }}
),

gps as (
    select
        id::text as id,
        source,
        date_derniere_connexion,
        prenom,
        nom,
        type_utilisateur,
        date_inscription,
        email,
        'NA'     as admin, -- noqa: references.keywords
        type_utilisateur_detail,
        departement_structure,
        type_structure,
        nom_structure
    from {{ ref('int_gps__tous_les_pros') }}
),

pilotage as (
    select
        id::text as id,
        source,
        date_derniere_connexion,
        prenom,
        nom,
        type_utilisateur,
        date_inscription,
        email,
        'NA'     as admin, -- noqa: references.keywords
        type_utilisateur_detail,
        departement_structure,
        type_structure,
        nom_structure
    from {{ ref('int_pilotage__tous_les_pros') }}
),

marche as (
    select
        id::text as id,
        source,
        date_derniere_connexion,
        prenom,
        nom,
        type_utilisateur,
        date_inscription,
        email,
        admin,
        type_utilisateur_detail,
        departement_structure,
        type_structure,
        nom_structure
    from {{ ref('int_marche__tous_les_pros') }}
),

monrecap as (
    select
        id::text as id,
        source,
        date_derniere_connexion,
        prenom,
        nom,
        type_utilisateur,
        date_inscription,
        email,
        'NA'     as admin, -- noqa: references.keywords
        type_utilisateur_detail,
        departement_structure,
        type_structure,
        nom_structure
    from {{ ref('int_monrecap__tous_les_pros') }}
),

rdvi as (
    select
        id::text as id,
        source,
        date_derniere_connexion,
        prenom,
        nom,
        type_utilisateur,
        date_inscription,
        email,
        admin,
        type_utilisateur_detail,
        departement_structure,
        type_structure,
        nom_structure
    from {{ ref('int_rdvi__tous_les_pros') }}
),

final as (
    select * from dora
    union all
    select * from emplois
    union all
    select * from gps
    union all
    select * from pilotage
    union all
    select * from marche
    union all
    select * from monrecap
    union all
    select * from rdvi
)

select * from final
