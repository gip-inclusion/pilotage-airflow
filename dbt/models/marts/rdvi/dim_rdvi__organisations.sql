select
    id   as organisation_id,
    name as organisation_name
from {{ ref('stg_rdvi__organisations') }}
