with nb_candidats_recherche_active as (
    select
        (
            select count(distinct id)
            from stg_candidats_candidatures
            where date_candidature >= current_date - interval '6 months'
        ) as cdd_stg,
        (
            select count(*)
            from candidats_recherche_active
            where date_derniere_candidature >= current_date - interval '6 months'
        ) as cdd_candidats_recherche_active
)

select *
from nb_candidats_recherche_active
where cdd_stg != cdd_candidats_recherche_active
