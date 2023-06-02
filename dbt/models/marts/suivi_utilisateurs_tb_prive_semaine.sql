select
    visits.user_id                                                                                          as id_utilisateur,
    c1_users.email                                                                                          as email_utilisateur,
    metabase_ids.nom_tb                                                                                     as nom_tb,
    visits.department                                                                                       as departement,
    visits.region                                                                                           as region,
    case
        when visits.user_kind = 'prescriber' then 'prescripteur'
        when visits.user_kind = 'siae_staff' then 'siae'
        when visits.user_kind = 'labor_inspector' then 'institution'
        when visits.user_kind = 'itou_staff' then 'staff interne'
    end                                                                                                     as type_utilisateur,
    coalesce(organisations.type, structures.type, institutions.type)                                        as type_organisation,
    coalesce(organisations.nom, structures.nom, institutions.nom)                                           as nom_organisation,
    -- recover date of the monday of the current week
    date_trunc('day', visits.measured_at) - INTERVAL '1 day' * (extract('dow' from visits.measured_at) - 1) as semaine,
    date_part('week', visits.measured_at)                                                                   as num_semaine,
    count(distinct visits.measured_at)                                                                      as nb_visites
from {{ source('emplois', 'c1_private_dashboard_visits_v0') }} as visits
left join {{ ref('metabase_dashboards') }} as metabase_ids
    on metabase_ids.id_tb = cast(visits.dashboard_id as INTEGER)
left join {{ source('emplois', 'organisations') }} as organisations
    on organisations.id = cast(visits.current_prescriber_organization_id as INTEGER) and visits.user_kind = 'prescriber'
left join {{ source('emplois', 'structures') }} as structures
    on structures.id = cast(visits.current_siae_id as INTEGER) and visits.user_kind = 'siae_staff'
left join {{ source('emplois', 'institutions') }} as institutions
    on institutions.id = cast(visits.current_institution_id as INTEGER) and visits.user_kind = 'labor_inspector'
left join {{ source('emplois', 'utilisateurs') }} as c1_users
    on c1_users.id = cast(visits.user_id as INTEGER)
group by
    id_utilisateur,
    email_utilisateur,
    nom_tb,
    departement,
    region,
    type_utilisateur,
    type_organisation,
    nom_organisation,
    semaine,
    num_semaine
