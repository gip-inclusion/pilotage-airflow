with src as (
    select * from {{ ref('seed_insee_epci') }}
)

select
    epci::text as code_insee_epci,
    libepci    as nom_epci,
    nature_epci
from src
