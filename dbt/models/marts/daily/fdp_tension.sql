select
    {{ pilo_star(ref('fdp_tension_12')) }}
from {{ ref('fdp_tension_12') }}
union all
select
    {{ pilo_star(ref('fdp_tension_34')) }}
from {{ ref('fdp_tension_34') }}
union all
select
    {{ pilo_star(ref('fdp_tension_56')) }}
from {{ ref('fdp_tension_56') }}
