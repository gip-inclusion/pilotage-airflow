with src_naf as (
    select * from {{ ref('insee_secteur_activite_naf') }}
)

select
    btrim(("NAF 2025 divisions")::text) as code_secteur_activite,
    btrim(("Intitulés")::text)         as secteur_activite
from src_naf
