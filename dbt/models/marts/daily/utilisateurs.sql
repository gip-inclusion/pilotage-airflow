with users as (
    select * from {{ ref('stg_emplois__utilisateurs') }}
),

collaborations as (
    select * from {{ ref('stg_emplois__collaborations') }}
),

final as (
    select
        users.id,
        users.uid,
        users.email,
        users.type,
        users.prenom,
        users.nom,
        users.derniere_connexion        as "dernière_connexion", --we kept the accents because they are used elsewhere
        users.date_inscription,
        users.date_mise_a_jour_metabase as "date_mise_à_jour_metabase",
        collaborations.id_structure,
        collaborations.id_organisation,
        collaborations.id_institution,
        collaborations.administrateur
    from users
    left join collaborations on users.id = collaborations.id_utilisateur
)

select * from final
