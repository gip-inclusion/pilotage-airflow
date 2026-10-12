with suspensions as (
    select
        *,
        row_number() over (
            partition by id_pass_iae
            order by date_debut desc nulls last, id_suspension desc
        ) as rang
    from {{ ref('stg_emplois__suspensions') }}
)

select
    id_pass_iae,
    count(*)                              as nombre_suspensions,
    min(date_debut)                       as date_premiere_suspension,
    max(date_debut)                       as date_derniere_suspension,
    max(date_fin) filter (where rang = 1) as date_de_fin_derniere_suspension,
    bool_or(en_cours)                     as is_suspension_en_cours
from suspensions
group by id_pass_iae
