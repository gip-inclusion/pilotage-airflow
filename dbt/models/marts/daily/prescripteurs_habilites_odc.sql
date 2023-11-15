select {{ pilo_star(ref('stg_prescripteurs_odc')) }}
from {{ ref('stg_prescripteurs_odc') }}
