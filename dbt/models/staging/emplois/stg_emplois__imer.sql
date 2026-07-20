select
    id,
    date,
    user_session,
    user_kind,
    user_id,
    user_prescriber_organization_id,
    user_company_id,
    structure_id,
    kind,
    service_id,
    source,
    external_link,
    orientation_id,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'imer_v0') }}
