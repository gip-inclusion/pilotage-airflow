select
    {{ pilo_star(ref('stg_organisations')) }}
from {{ ref('stg_organisations') }}
where type in ('CHRS', 'CHU', 'RS_FJT', 'OIL')
