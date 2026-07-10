select
    id,
    user_id,
    rdv_id,
    follow_up_id,
    status,
    rdv_solidarites_participation_id,
    convocable::boolean                    as convocable,
    created_by_type,
    created_by_agent_prescripteur::boolean as created_by_agent_prescripteur,
    france_travail_id
from {{ source('rdv_insertion', 'participations') }}
