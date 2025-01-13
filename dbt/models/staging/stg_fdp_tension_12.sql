select
    {{ pilo_star(ref('stg_fdp')) }},
    '1- Fiches de poste' as etape,
    nb_fdp_struct        as valeur
from stg_fdp
union all
select
    {{ pilo_star(ref('stg_fdp_actives')) }},
    '2- Fiches de poste actives' as etape,
    nb_fdp_struct                as valeur
from stg_fdp_actives
