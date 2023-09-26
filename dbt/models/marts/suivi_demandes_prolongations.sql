select
    {{ pilo_star(ref('stg_suivi_demandes_prolongations_acceptees')) }}
from
    {{ ref('stg_suivi_demandes_prolongations_acceptees') }}
union all
select
    {{ pilo_star(ref('stg_suivi_demandes_prolongations_non_acceptees')) }}
from
    {{ ref('stg_suivi_demandes_prolongations_non_acceptees') }}
