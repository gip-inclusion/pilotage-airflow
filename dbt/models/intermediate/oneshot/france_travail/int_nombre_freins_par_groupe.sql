select
    {{ dbt_utils.star(ref('stg_ft_data_diag')) }},
    coalesce(n_nbcontraintesactives, 0) as total_freins_declares,
    case
        when n_nbcontraintesactives > 0 then nombre_demandeurs_emploi
        else 0
    end                                 as demandeurs_emploi_avec_freins
from {{ ref("stg_ft_data_diag") }}
