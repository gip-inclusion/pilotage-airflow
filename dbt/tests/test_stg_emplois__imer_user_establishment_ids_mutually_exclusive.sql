{{ config(severity='warn', tags=['dora']) }}

select
    id,
    user_prescriber_organization_id,
    user_company_id
from {{ ref('stg_emplois__imer') }}
where
    user_prescriber_organization_id is not null
    and user_company_id is not null
