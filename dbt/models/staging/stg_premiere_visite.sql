select
    user_id,
    dashboard_id,
    min(measured_at) as premiere_visite
from {{ source('emplois', 'c1_private_dashboard_visits_v0') }}
group by
    user_id,
    dashboard_id
