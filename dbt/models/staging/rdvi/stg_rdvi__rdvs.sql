select
    id,
    rdv_solidarites_rdv_id,
    starts_at::timestamp            as starts_at,
    duration_in_min::integer        as duration_in_min,
    organisation_id,
    motif_id,
    lieu_id,
    status,
    cancelled_at::timestamp         as cancelled_at,
    created_by,
    context,
    address,
    visio_url,
    uuid,
    time_zone,
    users_count::integer            as users_count,
    max_participants_count::integer as max_participants_count
from {{ source('rdv_insertion', 'rdvs') }}
