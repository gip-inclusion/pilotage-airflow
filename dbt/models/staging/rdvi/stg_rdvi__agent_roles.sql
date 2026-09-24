select
    id,
    agent_id,
    organisation_id,
    access_level,
    authorized_to_export_csv::boolean as authorized_to_export_csv
from {{ source('rdv_insertion', 'agent_roles') }}
