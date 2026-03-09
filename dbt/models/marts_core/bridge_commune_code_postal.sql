select distinct
    code_commune_insee,
    code_postal
from {{ ref('stg_insee_codes_postaux_commune') }}
