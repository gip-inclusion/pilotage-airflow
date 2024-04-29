select
    {{ pilo_star(ref('stg_candidatures')) }}
from {{ ref('stg_candidatures') }}
where structure_convergence = 'Oui'
