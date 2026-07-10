with invitations as (

    select *
    from {{ ref('stg_rdvi__invitations') }}

),

invitations_organisations as (

    select *
    from {{ ref('stg_rdvi__invitations_organisations') }}

),

organisations as (

    select *
    from {{ ref('stg_rdvi__organisations') }}

),

follow_ups as (

    select *
    from {{ ref('stg_rdvi__follow_ups') }}

),

participations as (

    select *
    from {{ ref('stg_rdvi__participations') }}

),

rdvs as (

    select *
    from {{ ref('stg_rdvi__rdvs') }}

),

motif_categories as (

    select *
    from {{ ref('stg_rdvi__motif_categories') }}

),

departments as (

    select *
    from {{ ref('stg_rdvi__departments') }}

),

inv_org as (

    select distinct on (invitations_organisations.invitation_id)
        invitations_organisations.invitation_id,
        organisations.organisation_type
    from invitations_organisations
    inner join organisations
        on invitations_organisations.organisation_id = organisations.id
    order by
        invitations_organisations.invitation_id,
        organisations.organisation_type nulls last

),

rdvi_invitations as (

    select
        motif_categories.motif_category_type,
        to_char(date_trunc('month', invitations.created_at), 'yyyy-mm')         as mois,
        coalesce(inv_org.organisation_type, 'autre')                            as organisation_type,
        coalesce(departments.number, 'Inconnu')                                 as raw_departement,
        count(distinct invitations.id)                                          as nb_invitations,
        count(distinct case when rdvs.uuid is not null then invitations.id end) as nb_with_rdv
    from invitations
    left join follow_ups
        on invitations.follow_up_id = follow_ups.id
    left join participations
        on invitations.follow_up_id = participations.follow_up_id
    left join rdvs
        on participations.rdv_id = rdvs.id
    left join motif_categories
        on follow_ups.motif_category_id = motif_categories.id
    left join inv_org
        on invitations.id = inv_org.invitation_id
    left join departments
        on invitations.department_id = departments.id
    where
        invitations.created_at >= date_trunc('month', current_date) - interval '14 months'
        and invitations.created_at < date_trunc('month', current_date)
    group by
        motif_categories.motif_category_type,
        to_char(date_trunc('month', invitations.created_at), 'yyyy-mm'),
        coalesce(inv_org.organisation_type, 'autre'),
        coalesce(departments.number, 'Inconnu')

),

mapped_invitations as (

    select
        mois,
        nb_invitations,
        nb_with_rdv,
        case motif_category_type
            when 'rsa_orientation' then 'Invitation à un RDV d’orientation'
            when 'rsa_accompagnement' then 'Invitation à un RDV d’accompagnement'
            when 'siae' then 'Invitation à un Entretien SIAE'
            when 'autre' then 'Invitation à un Autre RDV'
            else motif_category_type
        end                          as type_acte,
        case organisation_type
            when 'france_travail' then 'FRANCE_TRAVAIL'
            when 'delegataire_rsa' then 'DELEGATAIRE_RSA'
            when 'conseil_departemental' then 'CONSEIL_DEPARTEMENTAL'
            when 'siae' then 'SIAE'
            else 'Autre'
        end                          as type_structure,
        case
            when raw_departement is null or trim(raw_departement) = '' then 'Inconnu'
            when upper(split_part(trim(raw_departement), ' - ', 1)) in ('2A', '2B')
                then upper(split_part(trim(raw_departement), ' - ', 1))
            when
                split_part(trim(raw_departement), ' - ', 1) ~ '^[0-9]+$'
                and split_part(trim(raw_departement), ' - ', 1)::integer between 1 and 95
                then lpad(split_part(trim(raw_departement), ' - ', 1), 2, '0')
            when
                split_part(trim(raw_departement), ' - ', 1) ~ '^[0-9]+$'
                and split_part(trim(raw_departement), ' - ', 1)::integer between 971 and 976
                then split_part(trim(raw_departement), ' - ', 1)
            else 'Inconnu'
        end                          as departement,
        nb_invitations - nb_with_rdv as nb_without_rdv
    from rdvi_invitations

),

actes_metiers as (

    select
        mois,
        'rdvi'               as source,
        type_acte,
        'Accompagnement'     as categorie_acte,
        true                 as north_star,
        true                 as north_star_70,
        true                 as traite,
        type_structure,
        departement,
        nb_with_rdv::integer as nombre_actes
    from mapped_invitations
    where nb_with_rdv > 0

    union all

    select
        mois,
        'rdvi'                  as source,
        type_acte,
        'Accompagnement'        as categorie_acte,
        false                   as north_star,
        false                   as north_star_70,
        false                   as traite,
        type_structure,
        departement,
        nb_without_rdv::integer as nombre_actes
    from mapped_invitations
    where nb_without_rdv > 0

)

select *
from actes_metiers
