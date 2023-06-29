select {{ pilo_star(ref('stg_reseaux')) }}
from
    {{ ref('stg_reseaux') }}
