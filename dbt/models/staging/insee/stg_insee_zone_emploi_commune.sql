with src as (
    select * from {{ ref('seed_insee_zone_emploi_commune') }}
)

select
    ze2020::text               as code_zone_emploi,
    ze_partie_reg::text        as zone_emploi_partie_regionale,
    lpad(codgeo::text, 5, '0') as code_commune_insee
from src
