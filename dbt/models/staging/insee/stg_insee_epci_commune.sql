with src as (
    select * from {{ ref('seed_insee_epci_commune') }}
)

select
    epci::text                 as code_insee_epci,
    lpad(codgeo::text, 5, '0') as code_commune_insee
from src
