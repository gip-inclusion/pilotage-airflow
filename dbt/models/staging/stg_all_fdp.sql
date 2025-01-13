select
    {{ pilo_star(ref('stg_fdp_tension_12')) }}
from stg_fdp_tension_12
union all
select
    {{ pilo_star(ref('stg_fdp_tension_34')) }}
from stg_fdp_tension_34
union all
select
    {{ pilo_star(ref('stg_fdp_tension_56')) }}
from stg_fdp_tension_56
