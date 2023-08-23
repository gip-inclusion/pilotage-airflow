select
    {{ pilo_star(ref('stg_organisations')) }}
from
    {{ ref('stg_organisations') }}
