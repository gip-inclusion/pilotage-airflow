select
    visits.user_id                                                                                          as id_utilisateur,
    c1_users.email                                                                                          as email_utilisateur,
    metabase_ids.nom_tb,
    visits.department                                                                                       as departement,
    visits.region,
    case
        when visits.user_kind = 'prescriber' then 'prescripteur'
        when visits.user_kind = 'employer' then 'siae'
        when visits.user_kind = 'labor_inspector' then 'institution'
        when visits.user_kind = 'itou_staff' then 'staff interne'
    end                                                                                                     as type_utilisateur,
    coalesce(organisations.type, structures.type, institutions.type)                                        as type_organisation,
    coalesce(organisations.nom, structures.nom, institutions.nom)                                           as nom_organisation,
    -- recover date of the monday of the current week
    date_trunc('day', visits.measured_at) - INTERVAL '1 day' * (extract('dow' from visits.measured_at) - 1) as semaine,
    date_part('week', visits.measured_at)                                                                   as num_semaine,
    count(distinct visits.measured_at)                                                                      as nb_visites,
    case
        when visits.measured_at = first_visit.premiere_visite then 'Oui'
        else 'Non'
    end                                                                                                     as premiere_visite,
    case
        when visits.measured_at = max(first_visit_all_tbs.premiere_visite) then 'Oui'
        else 'Non'
    end                                                                                                     as premiere_visite_tous_tb
from {{ source('emplois', 'c1_private_dashboard_visits_v0') }} as visits
left join {{ ref('metabase_dashboards') }} as metabase_ids
    on metabase_ids.id_tb = cast(visits.dashboard_id as INTEGER)
left join {{ ref('stg_organisations') }} as organisations
    on organisations.id = cast(visits.current_prescriber_organization_id as INTEGER) and visits.user_kind = 'prescriber'
left join {{ ref('structures') }} as structures
    on structures.id = cast(visits.current_company_id as INTEGER) and visits.user_kind = 'employer'
left join {{ source('emplois', 'institutions') }} as institutions
    on institutions.id = cast(visits.current_institution_id as INTEGER) and visits.user_kind = 'labor_inspector'
left join {{ source('emplois', 'utilisateurs_v0') }} as c1_users
    on c1_users.id = cast(visits.user_id as INTEGER)
left join {{ ref('stg_premiere_visite') }} as first_visit
    on visits.user_id = first_visit.user_id and visits.dashboard_id = first_visit.dashboard_id
left join {{ ref('stg_premiere_visite_tous_tb') }} as first_visit_all_tbs
    on visits.user_id = first_visit_all_tbs.user_id
-- ignore intern staff and 119 dashboard (c1 intern stats)
where c1_users.email not in (select email from {{ ref('pilotage_c1_users') }}) and visits.dashboard_id != '119'
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
    num_semaine,
    visits.measured_at,
    first_visit.premiere_visite
