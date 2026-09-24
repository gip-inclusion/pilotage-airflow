with users as (
    select * from {{ ref('stg_emplois__utilisateurs') }}
),

collaborations as (
    select * from {{ ref('stg_emplois__collaborations') }}
),

organisations as (
    select * from {{ ref('stg_organisations') }}
),

structures as (
    select * from {{ ref('stg_structures') }}
),

institutions as (
    select * from {{ ref('stg_emplois__institutions') }}
),

final as (
    select distinct
        users.id,
        'Les emplois'            as source,
        users.derniere_connexion as date_derniere_connexion,
        users.prenom,
        users.nom,
        users.type               as type_utilisateur,
        users.date_inscription,
        users.email,
        case
            when collaborations.administrateur = 0 then 'Non'
            when collaborations.administrateur = 1 then 'Oui'
            else ''
        end                      as admin, -- noqa: RF04
        case
            when users.type = 'employer' then 'Employeur'
            when users.type = 'labor_inspector' then 'Institution'
            else coalesce(nullif(organisations.habilitation, ''), 'Prescripteur')
        end                      as type_utilisateur_detail,
        case
            when collaborations.id_organisation is not null then organisations."département"
            when collaborations.id_structure is not null then structures."département"
            else institutions.departement
        end                      as departement_structure,
        case
            when collaborations.id_organisation is not null then organisations.type
            when collaborations.id_structure is not null then structures.type_struct
            else institutions.type
        end                      as type_structure,
        case
            when collaborations.id_organisation is not null then organisations.nom
            when collaborations.id_structure is not null then structures.nom
            else institutions.nom
        end                      as nom_structure
    from users
    left join collaborations on users.id = collaborations.id_utilisateur
    left join organisations on collaborations.id_organisation = organisations.id
    left join structures on collaborations.id_structure = structures.id
    left join institutions on collaborations.id_institution = institutions.id
)

select * from final
