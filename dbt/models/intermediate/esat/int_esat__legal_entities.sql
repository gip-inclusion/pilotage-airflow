select distinct
    managing_organization_finess,
    managing_organization_name,
    managing_organization_legal_status_code,
    managing_organization_legal_status_label,
    managing_organization_email
from {{ ref('stg_esat__liste_officielle') }}
