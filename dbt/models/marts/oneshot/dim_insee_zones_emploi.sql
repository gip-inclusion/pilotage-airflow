with zones_emploi as (
    select * from {{ ref('stg_insee_zones_emploi') }}
)

select
    code_zone_emploi,
    zone_emploi
from zones_emploi
