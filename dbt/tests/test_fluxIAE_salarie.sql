with nb_salaries as (
    select
        (
            select count(*)
            from {{ ref("stg_salarie") }}
        ) as salarie_stg,
        (
            select count(distinct salarie_id)
            from {{ ref("fluxIAE_Salarie_v2") }}
        ) as salarie_distinct
)

select *
from nb_salaries
where salarie_stg != salarie_distinct
