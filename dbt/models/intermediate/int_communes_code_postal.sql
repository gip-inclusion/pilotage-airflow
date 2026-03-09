with codes_postaux as (
    select * from {{ ref('bridge_commune_code_postal') }}
)

select
    code_commune_insee,
    min(code_postal)            as code_postal_principal,
    count(distinct code_postal) as nb_codes_postaux
from codes_postaux
group by code_commune_insee
