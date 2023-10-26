select
    {{ pilo_star(ref('stg_fdp_difficulte_recrutement')) }}
from stg_fdp_difficulte_recrutement
where aucune_candidatures_recues
