with users as (
    select * from {{ ref('stg_marche__users') }}
),

siaes as (
    select * from {{ ref('stg_marche__siaes') }}
),

siae_admins as (
    select distinct admin_email
    from siaes
    where admin_email is not null
),

-- Un utilisateur peut être rattaché à plusieurs SIAE : on n'en garde qu'une par utilisateur,
-- pour servir de proxy à son département.
-- Impossible d'utiliser dim_communes aussi
user_siae as (
    select distinct on (siae_users.user_id)
        siae_users.user_id,
        siaes.department as departement_siae
    from {{ ref('stg_marche__siae_users') }} as siae_users
    inner join siaes on siae_users.siae_id = siaes.id
),

enriched as (
    select
        users.*,
        user_siae.departement_siae,
        case
            when users.buyer_kind_detail is null or users.buyer_kind_detail = '' then users.partner_kind
            else users.buyer_kind_detail
        end                                                                     as type_utilisateur_detail,
        case
            when users.company_name is null or users.company_name = '' then 'NA'
            else users.company_name
        end                                                                     as nom_structure,
        case
            when users.kind is null or users.kind = '' then 'NA'
            when users.kind = 'SIAE' then 'SIAE'
            when users.kind = 'BUYER' then 'Entreprise acheteuse'
            when users.kind = 'PARTNER' then 'Entreprise partenaire'
            when users.kind = 'INDIVIDUAL' then 'Particulier'
        end                                                                     as type_structure,
        case when siae_admins.admin_email is not null then 'Oui' else 'Non' end as admin, -- noqa: references.keywords
        -- Héritage de Pierre
        case
            when length(users.first_name) > 15 and lower(users.first_name) like '%nz%' then 'spam'
            else 'ok'
        end                                                                     as antispam_status
    from users
    left join siae_admins on users.email = siae_admins.admin_email
    left join user_siae on users.id = user_siae.user_id
),

final as (
    select
        id,
        'Le marché'                                  as source,
        last_login                                   as date_derniere_connexion,
        first_name                                   as prenom,
        last_name                                    as nom,
        kind                                         as type_utilisateur,
        date_joined                                  as date_inscription,
        email,
        admin,
        type_utilisateur_detail,
        type_structure,
        nom_structure,
        coalesce(nullif(departement_siae, ''), 'NA') as departement_structure
    from enriched
    where
        antispam_status = 'ok'
        and email is not null
        and email <> ''
        and type_structure is not null
)

select * from final
