with nb_salaries as (
    select
        (
            select count(distinct hash_nir)
            from {{ ref("beneficiaires") }}
        ) as beneficiaires,
        (
            select count(distinct hash_nir)
            from {{ ref("etp_par_salarie") }}
        ) as salarie_distinct
)

select *
from nb_salaries
where beneficiaires != salarie_distinct
