with src as (
    select * from {{ ref('seed_insee_zones_emploi') }}
)

select
    ze2020::text as code_zone_emploi,
    libze2020    as zone_emploi_2020
from src
