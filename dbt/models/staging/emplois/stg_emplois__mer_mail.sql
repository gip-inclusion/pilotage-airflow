select
    id,
    sent_at,
    answered_at,
    user_kind,
    user_id,
    user_prescriber_organization_id,
    user_company_id,
    imer_id::text               as imer_id,
    service_id,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'mer_mail_v0') }}
