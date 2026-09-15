{{ config(
    materialized='view'
) }}

with active_user_mobilisations as (
    select * from {{ ref('int_dora__mobilisationevent_user') }}
),

mobilisation_events as (
    select
        events.id,
        active_user_mobilisations.user_id,
        events.date,
        nullif(split_part(events.path, '/', 3), '') as service_slug
    from {{ ref('stg_dora__mobilisationevent') }} as events
    inner join active_user_mobilisations
        on events.id = active_user_mobilisations.mobilisation_id
)

select distinct on (m.id)
    m.id as mobilisation_id,
    o.orientation_id
from mobilisation_events as m
left join {{ ref('int_dora__orientation_user_service') }} as o
-- Les orientations qui suivent une mobilisation ont le même user_id, sont créées
-- dans l'heure qui suit la mobilisation et concernent le même service via le slug.
    on
        m.user_id = o.user_id
        and o.orientation_creation_date between m.date and m.date + INTERVAL '1 hour'
        and m.service_slug = o.service_slug
order by
    m.id asc,
    o.orientation_creation_date asc
