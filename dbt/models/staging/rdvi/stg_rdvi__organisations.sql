select
    id,
    rdv_solidarites_organisation_id,
    name,
    department_id,
    organisation_type,
    email,
    phone_number,
    safir_code,
    slug,
    website,
    archived_at::timestamp                     as archived_at,
    data_retention_duration_in_months::integer as data_retention_duration_in_months,
    display_in_stats::boolean                  as display_in_stats
from {{ source('rdv_insertion', 'organisations') }}
