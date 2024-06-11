with nb_utilisateurs as (
    select
        (
            select count(distinct id)
            from utilisateurs
        ) as utilisateurs,
        (
            select count(*)
            from utilisateurs_v0
        ) as utilisateurs_v0
)

select *
from nb_utilisateurs
where utilisateurs != utilisateurs_v0
