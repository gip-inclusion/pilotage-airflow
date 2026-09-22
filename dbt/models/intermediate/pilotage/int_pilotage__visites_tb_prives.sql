with visites as (
    select
        *,
        date_visite = min(date_visite) over (partition by id_utilisateur, id_tb) as premiere_visite_tb,
        date_visite = min(date_visite) over (partition by id_utilisateur)        as premiere_visite_tous_tb
    from {{ ref('stg_pilotage__visites') }}
),

users as (
    select * from {{ ref('stg_emplois__utilisateurs') }}
),

utilisateurs_internes as (
    select * from {{ ref('pilotage_c1_users') }} --seed
),

tableaux_de_bord as (
    select * from {{ ref('metabase_dashboards') }} --seed
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
    select
        visites.id,
        visites.id_utilisateur,
        users.email                                                             as email_utilisateur,
        visites.id_tb,
        tableaux_de_bord.nom_tb,
        visites.date_visite,
        visites.departement,
        visites.region,
        visites.premiere_visite_tb,
        visites.premiere_visite_tous_tb,
        case visites.type_utilisateur
            when 'prescriber' then 'prescripteur'
            when 'employer' then 'siae'
            when 'labor_inspector' then 'institution'
            when 'itou_staff' then 'staff interne'
        end                                                                     as type_utilisateur,
        coalesce(organisations.type, structures.type_struct, institutions.type) as type_organisation,
        coalesce(organisations.nom, structures.nom, institutions.nom)           as nom_organisation
    from visites
    inner join users on visites.id_utilisateur = users.id
    left join tableaux_de_bord on visites.id_tb = tableaux_de_bord.id_tb
    left join organisations
        on visites.id_organisation = organisations.id and visites.type_utilisateur = 'prescriber'
    left join structures
        on visites.id_structure = structures.id and visites.type_utilisateur = 'employer'
    left join institutions
        on visites.id_institution = institutions.id and visites.type_utilisateur = 'labor_inspector'
    -- exclut le staff interne et le TB 119 (stats internes des emplois)
    where
        users.email not in (select email from utilisateurs_internes)
        and visites.id_tb != 119
)

select * from final
