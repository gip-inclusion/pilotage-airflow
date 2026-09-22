with users as (
    select * from {{ ref('int_emplois__tous_les_pros') }}
),

visites as (
    select
        id_utilisateur,
        min(date_visite) as premiere_visite,
        max(date_visite) as derniere_visite
    from {{ ref('int_pilotage__visites_tb_prives') }}
    group by id_utilisateur
),

final as (
    select distinct
        users.id,
        'Pilotage'              as source,
        visites.derniere_visite as date_derniere_connexion,
        users.prenom,
        users.nom,
        users.type_utilisateur,
        visites.premiere_visite as date_inscription,
        users.email,
        users.admin,
        users.type_utilisateur_detail,
        users.departement_structure,
        users.type_structure,
        users.nom_structure
    from users
    inner join visites on users.id = visites.id_utilisateur
)

select * from final
