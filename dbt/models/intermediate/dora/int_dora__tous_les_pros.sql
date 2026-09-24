with users as (
    select * from {{ ref('stg_dora__user') }}
),

members as (
    select * from {{ ref('stg_dora__structure_member') }}
),

structures as (
    select * from {{ ref('stg_dora__structure') }}
),

final as (
    select distinct
        users.id,
        'Dora'                                               as source,
        users.last_login                                     as date_derniere_connexion,
        'NA'                                                 as type_utilisateur_detail,
        users.date_joined                                    as date_inscription,
        users.email,
        coalesce(nullif(users.first_name, ''), 'NA')         as prenom,
        coalesce(nullif(users.last_name, ''), 'NA')          as nom,
        coalesce(nullif(users.main_activity, ''), 'NA')      as type_utilisateur,
        case when users.is_manager then 'Oui' else 'Non' end as admin, -- noqa: references.keywords
        coalesce(nullif(structures.name, ''), 'NA')          as nom_structure,
        coalesce(nullif(structures.typology, ''), 'NA')      as type_structure,
        coalesce(nullif(structures.department, ''), 'NA')    as departement_structure
    from users
    left join members on users.id = members.user_id
    left join structures on members.structure_id = structures.id
)

select * from final
