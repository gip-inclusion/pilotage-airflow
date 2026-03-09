with src as (
    select * from {{ ref('seed_ft_clpe') }}
)

select distinct
    territoire_id::text            as code_ft_clpe,
    territoire_libelle             as nom_clpe,
    lpad(commune_id::text, 5, '0') as code_commune_insee
from src
