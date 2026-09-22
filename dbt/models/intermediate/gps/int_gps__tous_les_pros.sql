with users as (
    select * from {{ ref('int_emplois__tous_les_pros') }}
),

logs as (
    select
        user_id,
        min(timestamp) as first_seen,
        max(timestamp) as last_seen
    from {{ ref('stg_gps__log_data') }}
    group by user_id
),

final as (
    select distinct
        users.id,
        'GPS'           as source,
        logs.last_seen  as date_derniere_connexion,
        users.prenom,
        users.nom,
        users.type_utilisateur,
        logs.first_seen as date_inscription,
        users.email,
        users.admin,
        users.type_utilisateur_detail,
        users.departement_structure,
        users.type_structure,
        users.nom_structure
    from users
    inner join logs on users.id = logs.user_id
)

select * from final
