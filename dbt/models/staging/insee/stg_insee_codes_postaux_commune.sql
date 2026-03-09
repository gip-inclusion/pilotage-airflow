with src as (
    select * from {{ ref('seed_insee_code_postaux') }}
)

select
    lpad(code_commune_insee::text, 5, '0') as code_commune_insee,
    lpad(code_postal::text, 5, '0')        as code_postal
from src
