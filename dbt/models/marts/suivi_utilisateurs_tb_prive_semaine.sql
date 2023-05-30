select
    visits.user_id                        as id_utilisateur,
    visits.user_kind                      as type_utilisateur,
    visits.dashboard_id                   as id_tb,
    metabase_ids.nom_tb                   as nom_tb,
    visits.department                     as departement,
    visits.region                         as region,
    date_part('week', visits.measured_at) as semaine,
    count(distinct visits.measured_at)    as nb_visites
from {{ source('emplois', 'c1_private_dashboard_visits_v0') }} as visits
left join {{ ref('metabase_dashboards') }} as metabase_ids
    on metabase_ids.id_tb = cast(visits.dashboard_id as integer)
group by
    visits.user_id,
    visits.user_kind,
    visits.dashboard_id,
    nom_tb,
    visits.department,
    region,
    semaine
