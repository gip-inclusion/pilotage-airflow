{{ config(
    materialized='view'
) }}

with mobilisation_events as (
    select
        *,
        nullif(split_part(event_path, '/', 3), '') as service_slug
    from {{ ref('int_dora__mobilisationevent_user') }}
)

select distinct on (m.user_id, m.event_id)
    m.user_id,
    m.event_id                                 as mobilisation_id,
    o.orientation_id                           as first_following_orientation_id,
    m.event_date                               as mobilisation_date,
    o.orientation_creation_date                as first_following_orientation_date,
    o.orientation_creation_date - m.event_date as delay,
    o.orientation_id is not null               as generates_orientation
from mobilisation_events as m
left join {{ ref('int_dora__orientation_user_service') }} as o
-- Les orientations qui suivent une mobilisation ont le même user_id, sont créées
-- dans l'heure qui suit la mobilisation et concernent le même service via le slug.
    on
        m.user_id = o.user_id
        and o.orientation_creation_date between m.event_date and m.event_date + INTERVAL '1 hour'
        and m.service_slug = o.service_slug
order by
    m.user_id asc,
    m.event_id asc,
    o.orientation_creation_date asc
