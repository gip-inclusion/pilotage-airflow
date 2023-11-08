select
    md.nom_tb             as tableau_de_bord,
    md.type               as type_de_tb,
    cp.department         as "département",
    s."nom_département",
    cp.measured_at::date  as semaine,
    count(distinct cp.id) as visiteurs_uniques
from
    {{ source('emplois', 'c1_private_dashboard_visits_v0') }} as cp
left join {{ ref('structures') }} as s
    on cp.department = s."département"
left join {{ ref('metabase_dashboards') }} as md
    on (cp.dashboard_id)::integer = md.id_tb
group by
    md.nom_tb,
    md.type,
    cp.department,
    s."nom_département",
    cp.measured_at::date
