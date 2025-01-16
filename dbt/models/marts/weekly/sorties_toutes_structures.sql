select
    {{ pilo_star(ref('stg_sorties_aci_ei')) }}
from {{ ref('stg_sorties_aci_ei') }}
union all
select
    {{ pilo_star(ref('stg_sorties_ai_etti')) }}
from {{ ref('stg_sorties_ai_etti') }}
union all
select {{ pilo_star(ref('stg_sorties_eiti')) }}
from {{ ref('stg_sorties_eiti') }}
