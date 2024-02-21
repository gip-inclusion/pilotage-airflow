with suivi_marche as (
    select
        (
            select count(distinct identifiant_salarie)
            from {{ ref("marche_suivi_structure") }}
        ) as salaries_marche,
        (
            select count(distinct identifiant_salarie)
            from {{ ref("etp_par_salarie") }}
        ) as salaries_pilo
)

select *
from suivi_marche
where salaries_marche != salaries_pilo
