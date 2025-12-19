with src as (
    select * from {{ ref('stg_insee_secteur_activite_naf') }}
)

select
    code_secteur_activite,
    secteur_activite
from src
