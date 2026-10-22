with prolongations as (
    select
        *,
        row_number() over (
            partition by id_pass
            order by date_creation desc nulls last, id_prolongation desc
        ) as rang
    from {{ ref('stg_emplois__prolongations') }}
)

select
    id_pass,
    count(*)                                as nombre_prolongations,
    min(date_creation)                      as date_premiere_prolongation,
    max(date_creation)                      as date_derniere_prolongation,
    max(date_debut) filter (where rang = 1) as date_debut_derniere_prolongation,
    max(date_fin) filter (where rang = 1)   as date_fin_derniere_prolongation
from prolongations
group by id_pass
