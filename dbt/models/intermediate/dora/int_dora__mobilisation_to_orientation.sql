{{ config(
    materialized='view'
) }}

select distinct on (m.user_id, m.event_id)
    m.user_id,
    m.event_id                                 as mobilisation_id,
    o.orientation_id                           as first_following_orientation_id,
    m.event_date                               as mobilisation_date,
    o.orientation_creation_date                as first_following_orientation_date,
    o.orientation_creation_date - m.event_date as delay,
    o.orientation_id is not null               as generates_orientation
from {{ ref('int_dora__mobilisationevent_user') }} as m
left join {{ ref('int_dora__orientation_user_service') }} as o
-- orientations that follows mobilisations have the same user_id, are done in the hour after the mobilisation, and concern the same service (slug)
    on
        m.user_id = o.user_id
        and o.orientation_creation_date between m.event_date and m.event_date + INTERVAL '1 hour'
        and split_part(m.event_path, '/', 3) = o.service_slug
order by
    m.user_id asc,
    m.event_id asc,
    o.orientation_creation_date asc
