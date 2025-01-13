with nb_fdp_actives as (
    select
        (
            select sum(valeur)
            from {{ ref('fiches_deposte_en_tension_recrutement') }}
            where etape = '1- Fiches de poste '
        ) as fdpt_actives,
        (
            select count(distinct id_fdp)
            from {{ ref('stg_fdp_candidatures') }}
            where active
        ) as fdp_actives
)

select *
from nb_fdp_actives
where fdpt_actives != fdp_actives
