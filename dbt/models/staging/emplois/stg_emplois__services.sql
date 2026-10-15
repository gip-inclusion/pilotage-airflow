select
    id,
    uid,
    structure_uid,
    source,
    source_link,
    name,
    description,
    duration_weekly_hours,
    duration_weeks,
    code_insee,
    is_active::boolean          as is_active,
    "date_mise_à_jour_metabase" as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'services_v0') }}
