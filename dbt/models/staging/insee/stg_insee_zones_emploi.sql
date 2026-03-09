with zones_emploi as (
    select
        "ZE2020"::text as code_zone_emploi,
        "LIBZE2020"    as zone_emploi
    from {{ ref('insee_zones_emploi') }}
)

select
    code_zone_emploi,
    zone_emploi
from zones_emploi
