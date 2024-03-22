with nb_candidats as (
    select
        (
            select count(distinct id)
            from {{ ref("stg_candidats_candidatures") }}
        ) as nb_candidats_stg,
        (
            select count(*)
            from {{ ref("candidats") }}
        ) as nb_candidats
)

select *
from nb_candidats
where nb_candidats_stg != nb_candidats
