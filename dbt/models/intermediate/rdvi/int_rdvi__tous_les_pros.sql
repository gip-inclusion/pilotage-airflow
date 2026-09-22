with agents as (
    select * from {{ ref('stg_rdvi__agents') }}
),

agent_roles as (
    select * from {{ ref('stg_rdvi__agent_roles') }}
),

organisations as (
    select * from {{ ref('stg_rdvi__organisations') }}
),

--on ne peux pas utiliser dim_communes car il n'y a pas de code commune dans leur données
departments as (
    select * from {{ ref('stg_rdvi__departments') }}
),

final as (
    select
        agents.id,
        'RDV-i'                         as source,
        agents.last_sign_in_at          as date_derniere_connexion,
        -- le nom et le prénom vont donner des valeurs inexploitables
        -- je le laisse tel quel en espérant qu'un jour ces verrous sauteront.
        agents.first_name               as prenom,
        agents.last_name                as nom,
        organisations.organisation_type as type_utilisateur,
        agents.created_at               as date_inscription,
        agents.email,
        organisations.organisation_type as type_utilisateur_detail,
        departments.number              as departement_structure,
        organisations.organisation_type as type_structure,
        organisations.name              as nom_structure,
        case
            when agent_roles.authorized_to_export_csv or agents.super_admin then 'Oui'
            else 'Non'
        end                             as admin -- noqa: references.keywords
    from agents
    left join agent_roles on agents.id = agent_roles.agent_id
    left join organisations on agent_roles.organisation_id = organisations.id
    left join departments on organisations.department_id = departments.id
)

select * from final
