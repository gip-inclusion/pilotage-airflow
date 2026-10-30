select
    id,
    uid           as service_uid_di,
    structure_uid as structure_uid_di,
    source,
    source_link,
    name,
    description,
    duration_weekly_hours,
    duration_weeks,
    code_insee,
    is_active,
    date_mise_a_jour_metabase
from {{ ref('stg_emplois__services') }}
