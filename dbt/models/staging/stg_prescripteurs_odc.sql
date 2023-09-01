select {{ pilo_star(ref('stg_organisations')) }}
from {{ ref('stg_organisations') }}
where brsa = 1 or type = 'ODC' or type = 'DEPT'
