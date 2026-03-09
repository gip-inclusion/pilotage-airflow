with src as (
    select * from {{ref('seed_insee_zone_emploi_commune')}}
)
select
    codgeo::text as code_commune_insee,
    ze2020::text as code_zone_emploi,
    libze2020 as zone_emploi,
    ze_partie_reg::text as zone_emploi_partie_regionale
from src
