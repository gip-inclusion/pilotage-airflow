select
    {{ pilo_star(ref('stg_contrats'), except = ["nombre_heures_travail_zero"]) }}
from {{ ref('stg_contrats') }}
where nombre_heures_travail_zero = 'non'
