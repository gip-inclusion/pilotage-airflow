select
    {{ pilo_star(ref('stg_fdp_difficulte_recrutement')) }},
    '5- Fiches de poste en difficulté de recrutement' as etape,
    nb_fdp_struct                                     as valeur
from {{ ref('stg_fdp_difficulte_recrutement') }}
union all
select
    {{ pilo_star(ref('stg_fdp_difficulte_recrutement_sans_candidatures')) }},
    '6- Fiches de poste en difficulté de recrutement n ayant jamais reçu de candidatures' as etape,
    nb_fdp_struct                                                                         as valeur
from {{ ref('stg_fdp_difficulte_recrutement_sans_candidatures') }}
